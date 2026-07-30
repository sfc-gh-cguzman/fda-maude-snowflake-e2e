---
name: maude-deploy
description: Deploy the FDA MAUDE adverse-event pipeline + clinician Cortex Agents in any Snowflake account. Full medallion (RAW/CURATED/ANALYTICS), parallel backfill, semantic view, Cortex Search over 58M narratives, two agents with FDA source-URL citations.
---

# Deploy FDA MAUDE Pipeline

Deploys a Snowflake-native pipeline that ingests the FDA MAUDE device adverse-event
dataset, curates it into a star schema, and exposes it through Cortex Agents for
device-safety intelligence. All data comes from the openFDA public API (CC0, no
license or data-sharing agreement needed).

SQL is delivered as Jinja templates rendered against a per-deployment config, so the
pipeline can target any database / warehouse / role names.

## Prerequisites

- Active Snowflake connection
- A role that can CREATE DATABASE / WAREHOUSE, plus ACCOUNTADMIN for the External
  Access Integration and the account-level grants (EXECUTE MANAGED TASK, CORTEX_USER)
- `uv` available locally (the renderer needs `jinja2` + `pyyaml`)

## Step 1: Configuration

Ask the user for:

1. **Target database** (default: `MAUDE_DB`)
2. **Warehouse** (default: `MAUDE_WH`, created MEDIUM)
3. **Engineer role** (default: `MAUDE_ENGINEER`) and **clinician role** (default: `MAUDE_CLINICIAN`)
4. **Data scope** - how much history to load:

| Scope (`load_scope`) | Records | Lanes | Backfill | Search index | Total |
|---|---|---|---|---|---|
| `full` (1993-2026) | ~25.4M | 43 | ~45 min | ~30 min | ~75 min |
| `last_10_years` (2016+) | ~16M | 34 | ~20 min | ~15 min | ~35 min |
| `last_5_years` (2021+) | ~10M | 18 | ~12 min | ~10 min | ~22 min |

All scopes use the parallel fan-out (many XSMALL serverless lanes). Backfill is
idempotent and safe to re-run. Weekly incremental sync keeps data fresh regardless
of the initial scope.

Default to `last_10_years` if the user is unsure.

5. **AI enrichment sample size** (`enrichment_sample_rows`, default `200`).
   `AI_CLASSIFY` bills per token, so the shipped enrichment is a bounded sample,
   not the whole corpus. Raise it only if the user accepts the Cortex spend; the
   scale-up task pattern is documented at the bottom of `08_enrichment.sql`.

## Step 2: Write the config and render the SQL

Write the answers to a config JSON (start from `assets/config.sample.json`):

```json
{
  "target_database": "MAUDE_DB",
  "warehouse": "MAUDE_WH",
  "engineer_role": "MAUDE_ENGINEER",
  "clinician_role": "MAUDE_CLINICIAN",
  "eai_name": "MAUDE_OPENFDA_EAI",
  "target_lag": "1 day",
  "load_scope": "full",
  "enrichment_sample_rows": 200,
  "enrichment_min_corpus_rows": 100000
}
```

Render every object:

```bash
uv run --with jinja2 --with pyyaml python assets/renderer/render.py \
  --manifest assets/build_manifest.yaml \
  --templates assets/templates \
  --out ./rendered \
  --config ./config.json
```

The renderer is a pure text transform (no database access). It prints the resolved
config and the list of rendered files. Verify the output contains no leftover
default names if the user chose custom ones:

```bash
grep -l 'MAUDE_DB' ./rendered/*.sql   # expect no matches for a custom target_database
```

## Step 3: Execute in manifest order

Execute the rendered SQL in `build_manifest.yaml` `depends_on` order:

| Order | File | What it creates | Role needed |
|---|---|---|---|
| 1 | `00_setup.sql` | DB, schemas, warehouse, roles, EAI, task + CORTEX_USER grants | ACCOUNTADMIN |
| 2 | `01_raw.sql` | Stage, file format, VARIANT landing table, control ledgers | engineer |
| 3 | `02_ingest.sql` | `SP_MAUDE_INGEST` proc, weekly sync task, backfill task | engineer |
| 4 | `07_backfill_fanout.sql` | Launches the parallel backfill lanes (async) | engineer |
| 5 | `03_curated.sql` | Star schema Dynamic Tables | engineer |
| 6 | `04_analytics.sql` | MDR-grain views, semantic view, Cortex Search, AI enrichment DDL | engineer |
| 7 | `05_agents.sql` | Both Cortex Agents + clinician grants | engineer |
| 8 | `06_profiling.sql` | Profiling queries + DMFs | engineer |

Step 4 (the fan-out) runs **async**. Proceed to steps 5-8 while it loads.

`08_enrichment.sql` is **not** in this list on purpose — it runs in Step 4 below,
after the backfill is verified. Running it here would classify a near-empty
`EVENT_NARRATIVE` and ship a dead table.

**Monitor the backfill:**
```sql
SELECT status, COUNT(*) AS partitions, SUM(loaded_records) AS rows_loaded
FROM <TARGET_DB>.RAW.LOAD_CONTROL GROUP BY status;
```

## Step 4: Verify and finalize

Once `LOAD_CONTROL` shows all partitions LOADED with 0 FAILED:

1. **Reconcile** the load against the openFDA manifest:
```sql
SELECT
  (SELECT COUNT(*) FROM <TARGET_DB>.RAW.RAW_DEVICE_EVENT) AS raw_rows,
  (SELECT SUM(loaded_records) FROM <TARGET_DB>.RAW.LOAD_CONTROL WHERE status='LOADED') AS ledger_rows,
  (SELECT MAX(total_records) FROM <TARGET_DB>.RAW.MANIFEST_HISTORY) AS manifest_total;
```
For a `full` load these should match exactly. For a scoped load, `raw_rows` will be
less than `manifest_total` by design.

2. **Refresh the Dynamic Tables** (or wait for the target lag):
```sql
ALTER DYNAMIC TABLE <TARGET_DB>.CURATED.FACT_ADVERSE_EVENT REFRESH;
ALTER DYNAMIC TABLE <TARGET_DB>.CURATED.DIM_DEVICE REFRESH;
ALTER DYNAMIC TABLE <TARGET_DB>.CURATED.EVENT_NARRATIVE REFRESH;
ALTER DYNAMIC TABLE <TARGET_DB>.CURATED.PATIENT_OUTCOME REFRESH;
ALTER DYNAMIC TABLE <TARGET_DB>.CURATED.BRIDGE_DEVICE_PROBLEM REFRESH;
```

3. **Confirm the search service is serving:**
```sql
SHOW CORTEX SEARCH SERVICES LIKE 'MAUDE_NARRATIVE_SEARCH' IN SCHEMA <TARGET_DB>.ANALYTICS;
-- serving_state must be ACTIVE (INITIALIZING means the index is still building)
```
If it stays `INITIALIZING` with `source_data_num_rows = 0` for over an hour, DROP and
re-create the service - the initial index build can wedge.

4. **Resume the weekly sync:**
```sql
ALTER TASK <TARGET_DB>.RAW.TASK_MAUDE_WEEKLY_SYNC RESUME;
```

5. **Drop the transient backfill lanes:**
```sql
DECLARE i INT; BEGIN FOR i IN 0 TO 50 DO
  EXECUTE IMMEDIATE 'DROP TASK IF EXISTS <TARGET_DB>.RAW.TASK_MAUDE_BF_'||i;
END FOR; END;
```

6. **Populate the AI enrichment** — now that the corpus is loaded, run the
   deferred `08_enrichment.sql`. It self-guards: if `EVENT_NARRATIVE` is still
   below `enrichment_min_corpus_rows` it fails fast rather than sampling an
   empty table. Verify it landed:
```sql
SELECT COUNT(*) AS enriched_rows,
       COUNT(DISTINCT mdr_text_key) AS distinct_segments
FROM <TARGET_DB>.ANALYTICS.AI_EVENT_ENRICHMENT;
-- enriched_rows should equal enrichment_sample_rows (default 200)
-- and equal distinct_segments (grain is one row per narrative segment)
```

7. **Confirm citation IDs are unique** — agents cite on `MDR_TEXT_KEY`, the
   FDA-assigned per-segment key. `mdr_report_key` is NOT unique in this corpus
   (an MDR carries many narrative segments), so citing on it collapses or
   mis-attributes results:
```sql
SELECT COUNT(*) AS rows_, COUNT(DISTINCT mdr_text_key) AS ids
FROM <TARGET_DB>.CURATED.EVENT_NARRATIVE
WHERE narrative_text IS NOT NULL AND narrative_length >= 20;
-- rows_ must equal ids
```

8. **Test an agent** - in Snowsight (AI & ML > Agents) or via `/cortex-agent` chat
   against `<TARGET_DB>.ANALYTICS.MAUDE_DEVICE_SAFETY_AGENT`.

## What gets deployed

| Object | Schema | Purpose |
|---|---|---|
| SP_MAUDE_INGEST | RAW | Snowpark ingest proc (backfill + sync) |
| TASK_MAUDE_WEEKLY_SYNC | RAW | Weekly incremental (Mondays 6am PT) |
| RAW_DEVICE_EVENT | RAW | VARIANT landing, 1 row per MDR |
| LOAD_CONTROL / MANIFEST_HISTORY | RAW | Per-partition ledger + manifest snapshots |
| FACT_ADVERSE_EVENT | CURATED | Master event grain (DT) |
| DIM_DEVICE / V_DEVICE_PRIMARY | CURATED | Device detail + MDR-grain view |
| EVENT_NARRATIVE | CURATED | Narrative segments + `mdr_text_key` unique key + redaction flag (DT) |
| PATIENT_OUTCOME / V_PATIENT_PRIMARY | CURATED | Patient outcomes + MDR-grain view |
| BRIDGE_DEVICE_PROBLEM | CURATED | Product-problem codes (DT) |
| MAUDE_SAFETY_SV | ANALYTICS | Semantic view (Cortex Analyst) |
| MAUDE_NARRATIVE_SEARCH | ANALYTICS | Cortex Search, cited on `mdr_text_key` with FDA deep link |
| AI_EVENT_ENRICHMENT | ANALYTICS | AI_CLASSIFY failure mode + severity, per narrative segment (populated in Step 4) |
| MAUDE_DEVICE_SAFETY_AGENT | ANALYTICS | Analyst + Search + charts |
| MAUDE_FAILURE_MODE_AGENT | ANALYTICS | Narrative RAG |

## Credits and cost

- **Backfill (one-time):** many XSMALL serverless lanes for the duration in the scope table. Roughly 2-5 credits depending on scope.
- **Weekly sync:** XS serverless, seconds of compute per run. Negligible.
- **Dynamic Tables:** refresh on the configured target lag; incremental where possible.
- **Cortex Search:** serverless indexing, no warehouse credits.
- **Agents:** per-query Cortex consumption.
- **Source data:** free. openFDA is public domain (CC0), no API key needed for bulk downloads.

## Governance

Every agent response carries the FDA disclaimer: "MAUDE is a passive surveillance
system. Report counts cannot establish event rates, incidence, or causation, and this
information must not be used for individual patient-care decisions." Citations are keyed
on `mdr_text_key` (unique per narrative segment) and carry `mdr_report_key`,
`report_number`, and a `source_url` pointing at the openFDA API record for independent
verification.

## Reference docs (bundled)

- `references/overview.md` — personas, use cases, sample questions (structured / unstructured / hybrid).
- `references/architecture.md` — full design: dataset facts, ingestion mechanics, parallel backfill timings, star schema, verification checklist.
