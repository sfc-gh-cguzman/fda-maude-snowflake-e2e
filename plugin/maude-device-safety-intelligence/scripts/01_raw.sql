-- =====================================================================
-- MAUDE Ingestion - 01_raw.sql
-- Bronze layer: internal stage, raw VARIANT landing table, and the
-- control tables that drive idempotent backfill + incremental sync.
--
-- Run as MAUDE_ENGINEER. Idempotent.
-- =====================================================================

USE ROLE MAUDE_ENGINEER;
USE WAREHOUSE MAUDE_WH;
USE DATABASE MAUDE_DB;
USE SCHEMA RAW;

-- ---------------------------------------------------------------------
-- Internal stage for the downloaded openFDA partition files.
-- The ingest proc unzips each partition and PUTs the .json here; COPY
-- reads from it. Directory table on for visibility / debugging.
-- ---------------------------------------------------------------------
CREATE STAGE IF NOT EXISTS MAUDE_STAGE
  DIRECTORY = (ENABLE = TRUE)
  COMMENT = 'Landing stage for openFDA device/event JSON partitions';

-- ---------------------------------------------------------------------
-- JSON file format. openFDA files are a single object:
--   { "meta": {...}, "results": [ {mdr}, {mdr}, ... ] }
-- We load the whole file as one VARIANT row, then the COPY transform
-- flattens results[] into one row per MDR. STRIP_OUTER_ARRAY = FALSE
-- because the top level is an object, not an array.
-- ---------------------------------------------------------------------
CREATE FILE FORMAT IF NOT EXISTS FF_JSON_MAUDE
  TYPE = JSON
  STRIP_OUTER_ARRAY = FALSE
  COMMENT = 'openFDA device/event JSON (object with meta + results[])';

-- ---------------------------------------------------------------------
-- Raw landing table: one row per MDR record.
--   payload      - the full MDR JSON object (VARIANT)
--   report_key   - extracted mdr_report_key for dedup / joins downstream
--   _partition   - openFDA quarter partition, e.g. '2019q3'
--   _export_date - manifest export_date for that partition at load time
--   _src_file    - source zip file name
--   _loaded_at   - load timestamp
-- Clustered by partition so idempotent per-partition delete/reload and
-- downstream incremental Dynamic Tables prune efficiently.
-- ---------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS RAW_DEVICE_EVENT (
  payload       VARIANT,
  report_key    STRING,
  _partition    STRING,
  _export_date  DATE,
  _src_file     STRING,
  _loaded_at    TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
)
CLUSTER BY (_partition)
COMMENT = 'Bronze landing: one row per MDR from openFDA device/event';

-- ---------------------------------------------------------------------
-- LOAD_CONTROL: the source of truth for what has been loaded.
-- The sync proc diffs the live manifest against this to decide which
-- partitions to (re)load. One row per partition.
-- ---------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS LOAD_CONTROL (
  partition        STRING,          -- e.g. '2019q3'
  file_url         STRING,          -- source zip URL
  manifest_records NUMBER,          -- record count reported by manifest
  loaded_records   NUMBER,          -- rows actually landed
  export_date      DATE,            -- manifest export_date at load time
  status           STRING,          -- LOADED | FAILED | PENDING
  error_message    STRING,
  load_ts          TIMESTAMP_NTZ,
  CONSTRAINT pk_load_control PRIMARY KEY (partition)
)
COMMENT = 'Per-partition load ledger driving idempotent backfill + sync';

-- ---------------------------------------------------------------------
-- MANIFEST_HISTORY: full snapshot of download.json per sync run, for
-- audit and to detect endpoint-level export_date bumps.
-- ---------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS MANIFEST_HISTORY (
  captured_at        TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
  endpoint_export_dt DATE,
  total_records      NUMBER,
  partition_count    NUMBER,
  raw_manifest       VARIANT
)
COMMENT = 'Snapshot of openFDA device/event manifest per sync run';
