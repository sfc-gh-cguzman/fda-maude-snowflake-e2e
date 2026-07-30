-- =====================================================================
-- MAUDE Ingestion - 02_ingest.sql
-- Snowpark Python ingest procedure + weekly sync Task.
--
-- SP_MAUDE_INGEST(mode, max_partitions, quarter_like) drives BOTH:
--   mode = 'BACKFILL' -> load every partition not yet LOADED
--   mode = 'SYNC'     -> reload only partitions whose manifest
--                        export_date or record count changed (late/
--                        supplemental reports), plus brand-new quarters
--
-- Robustness notes:
--   * openFDA files are { "meta":..., "results":[...] }. A single MDR
--     object is small, but a whole quarter file can exceed the 16 MB
--     VARIANT limit and COPY cannot FLATTEN. So the proc STREAM-parses
--     results[] with ijson and writes NDJSON (one MDR per line) to the
--     stage; COPY then lands one VARIANT row per MDR. Memory stays flat.
--   * Idempotent per partition: deterministic staged filename +
--     overwrite, DELETE WHERE _partition, then COPY FORCE.
--     (REMOVE is not a supported statement type inside a stored proc.)
--   * All object references are fully qualified because the proc runs
--     with owner's rights.
--   * Every partition outcome is recorded in LOAD_CONTROL.
--
-- Run as MAUDE_ENGINEER. Idempotent (CREATE OR REPLACE).
-- =====================================================================

USE ROLE MAUDE_ENGINEER;
USE WAREHOUSE MAUDE_WH;
USE DATABASE MAUDE_DB;
USE SCHEMA RAW;

CREATE OR REPLACE PROCEDURE SP_MAUDE_INGEST(
    MODE           STRING DEFAULT 'BACKFILL',   -- 'BACKFILL' | 'SYNC'
    MAX_PARTITIONS  NUMBER DEFAULT 0,            -- 0 = no limit (process all in scope)
    QUARTER_LIKE    STRING DEFAULT NULL          -- optional filter, e.g. '2025q%' for validation
)
RETURNS VARIANT
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = ('snowflake-snowpark-python', 'requests', 'ijson')
HANDLER = 'run'
EXTERNAL_ACCESS_INTEGRATIONS = (MAUDE_OPENFDA_EAI)
COMMENT = 'openFDA device/event backfill + incremental sync (NDJSON stream load)'
AS
$$
import io, re, gzip, json, tempfile, os, datetime
import requests
import ijson

MANIFEST_URL = "https://api.fda.gov/download.json"

def _quarter_from_url(url):
    # .../device/event/2019q3/device-event-0001-of-0001.json.zip -> '2019q3-0001'
    m = re.search(r"/device/event/([^/]+)/(device-event-(\d+)-of-\d+)", url)
    if not m:
        return url.rsplit("/", 1)[-1].replace(".json.zip", "")
    quarter, _, part = m.group(1), m.group(2), m.group(3)
    return f"{quarter}-{part}"

def _fetch_manifest():
    r = requests.get(MANIFEST_URL, timeout=120)
    r.raise_for_status()
    return r.json()["results"]["device"]["event"]

def _download_and_stage(session, url, partition):
    # Stream the zip to a local temp file (bounded memory)
    tmp_zip = tempfile.NamedTemporaryFile(delete=False, suffix=".zip")
    try:
        with requests.get(url, stream=True, timeout=600) as resp:
            resp.raise_for_status()
            for chunk in resp.iter_content(chunk_size=1 << 20):
                tmp_zip.write(chunk)
        tmp_zip.close()
        import zipfile
        # deterministic filename per partition so overwrite=True is idempotent
        # (REMOVE is not a supported statement type inside a stored procedure)
        ndjson_path = os.path.join(tempfile.gettempdir(), f"{partition}.json.gz")
        rec = 0
        with zipfile.ZipFile(tmp_zip.name) as zf:
            inner = [n for n in zf.namelist() if n.lower().endswith(".json")][0]
            with zf.open(inner) as jf, gzip.open(ndjson_path, "wt", encoding="utf-8") as out:
                for obj in ijson.items(jf, "results.item"):
                    out.write(json.dumps(obj)); out.write("\n"); rec += 1
        stage_dir = f"@MAUDE_DB.RAW.MAUDE_STAGE/{partition}"
        session.file.put(ndjson_path, stage_dir, auto_compress=False, overwrite=True)
        os.unlink(ndjson_path)
        return rec, os.path.basename(url)
    finally:
        try: os.unlink(tmp_zip.name)
        except OSError: pass

def _copy_partition(session, partition, export_date, src_file):
    stage_dir = f"@MAUDE_DB.RAW.MAUDE_STAGE/{partition}"
    session.sql("DELETE FROM MAUDE_DB.RAW.RAW_DEVICE_EVENT WHERE _partition = ?", params=[partition]).collect()
    copy_sql = f"""
        COPY INTO MAUDE_DB.RAW.RAW_DEVICE_EVENT
            (payload, report_key, _partition, _export_date, _src_file)
        FROM (
            SELECT $1,
                   $1:mdr_report_key::string,
                   '{partition}',
                   '{export_date}'::date,
                   '{src_file}'
            FROM {stage_dir}/{partition}.json.gz
        )
        FILE_FORMAT = (FORMAT_NAME = MAUDE_DB.RAW.FF_JSON_MAUDE)
        FORCE = TRUE
        ON_ERROR = ABORT_STATEMENT
    """
    session.sql(copy_sql).collect()
    n = session.sql(
        "SELECT COUNT(*) AS c FROM MAUDE_DB.RAW.RAW_DEVICE_EVENT WHERE _partition = ?",
        params=[partition]).collect()[0]["C"]
    return n

def _upsert_control(session, partition, url, man_recs, loaded, export_date, status, err):
    session.sql(
        """
        MERGE INTO MAUDE_DB.RAW.LOAD_CONTROL t
        USING (SELECT ? AS partition) s ON t.partition = s.partition
        WHEN MATCHED THEN UPDATE SET
            file_url = ?, manifest_records = ?, loaded_records = ?,
            export_date = ?, status = ?, error_message = ?, load_ts = CURRENT_TIMESTAMP()
        WHEN NOT MATCHED THEN INSERT
            (partition, file_url, manifest_records, loaded_records, export_date, status, error_message, load_ts)
            VALUES (?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP())
        """,
        params=[partition, url, man_recs, loaded, export_date, status, err,
                partition, url, man_recs, loaded, export_date, status, err]).collect()

def run(session, MODE, MAX_PARTITIONS, QUARTER_LIKE):
    ev = _fetch_manifest()
    endpoint_export = ev["export_date"]
    parts = ev["partitions"]

    session.sql(
        """INSERT INTO MAUDE_DB.RAW.MANIFEST_HISTORY
           (endpoint_export_dt, total_records, partition_count, raw_manifest)
           SELECT ?::date, ?, ?, PARSE_JSON(?)""",
        params=[endpoint_export, ev["total_records"], len(parts), json.dumps(ev)]).collect()

    ctrl = {r["PARTITION"]: r for r in session.sql(
        "SELECT partition, export_date, manifest_records, status FROM MAUDE_DB.RAW.LOAD_CONTROL").collect()}

    work = []
    for p in parts:
        url = p["file"]; partition = _quarter_from_url(url)
        if QUARTER_LIKE and not re.match("^" + QUARTER_LIKE.replace("%", ".*") + "$", partition):
            continue
        man_recs = int(p.get("records") or 0); prior = ctrl.get(partition)
        if MODE.upper() == "BACKFILL":
            if prior is None or prior["STATUS"] != "LOADED":
                work.append((partition, url, man_recs))
        else:  # SYNC
            changed = (prior is None
                       or str(prior["EXPORT_DATE"]) != str(endpoint_export)
                       or int(prior["MANIFEST_RECORDS"] or -1) != man_recs
                       or prior["STATUS"] != "LOADED")
            if changed:
                work.append((partition, url, man_recs))

    if MAX_PARTITIONS and MAX_PARTITIONS > 0:
        work = work[: int(MAX_PARTITIONS)]

    loaded_ok, failed, total_rows = 0, 0, 0; errors = []
    for partition, url, man_recs in work:
        try:
            rec, src = _download_and_stage(session, url, partition)
            n = _copy_partition(session, partition, endpoint_export, src)
            _upsert_control(session, partition, url, man_recs, n, endpoint_export, "LOADED", None)
            loaded_ok += 1; total_rows += n
        except Exception as e:
            failed += 1; msg = str(e)[:1000]; errors.append({"partition": partition, "error": msg})
            _upsert_control(session, partition, url, man_recs, 0, endpoint_export, "FAILED", msg)

    return {"mode": MODE, "endpoint_export_date": endpoint_export,
            "manifest_total_records": ev["total_records"], "partitions_in_scope": len(work),
            "partitions_loaded": loaded_ok, "partitions_failed": failed,
            "rows_loaded_this_run": total_rows, "errors": errors[:20]}
$$;

-- ---------------------------------------------------------------------
-- Weekly incremental sync Task (serverless). Created SUSPENDED; resume
-- after the initial backfill completes. openFDA refreshes weekly.
-- ---------------------------------------------------------------------
CREATE OR REPLACE TASK TASK_MAUDE_WEEKLY_SYNC
  SCHEDULE = 'USING CRON 0 6 * * 1 America/Los_Angeles'   -- Mondays 06:00 PT
  USER_TASK_MANAGED_INITIAL_WAREHOUSE_SIZE = 'MEDIUM'
  COMMENT = 'Weekly openFDA MAUDE incremental sync'
AS
  CALL MAUDE_DB.RAW.SP_MAUDE_INGEST('SYNC', 0, NULL);

-- Resume once backfill is done:
-- ALTER TASK TASK_MAUDE_WEEKLY_SYNC RESUME;

-- ---------------------------------------------------------------------
-- Backfill usage (run in batches so each call stays within statement
-- timeout; the proc is idempotent and skips already-LOADED partitions):
--   CALL SP_MAUDE_INGEST('BACKFILL', 50, NULL);   -- repeat until 0 loaded
-- Or a single long-running call:
--   CALL SP_MAUDE_INGEST('BACKFILL', 0, NULL);
--
-- Preferred for full history: a one-off async task (24h timeout) so the
-- ~17.6 GB / 25.4M-row load runs unattended.
--   EXECUTE TASK TASK_MAUDE_BACKFILL;
--   -- monitor:
--   SELECT status, COUNT(*) FROM MAUDE_DB.RAW.LOAD_CONTROL GROUP BY status;
-- ---------------------------------------------------------------------
CREATE OR REPLACE TASK TASK_MAUDE_BACKFILL
  USER_TASK_MANAGED_INITIAL_WAREHOUSE_SIZE = 'MEDIUM'
  USER_TASK_TIMEOUT_MS = 86400000               -- 24h
  SUSPEND_TASK_AFTER_NUM_FAILURES = 0
  COMMENT = 'One-off full historical backfill of openFDA MAUDE (run via EXECUTE TASK)'
AS
  CALL MAUDE_DB.RAW.SP_MAUDE_INGEST('BACKFILL', 0, NULL);
