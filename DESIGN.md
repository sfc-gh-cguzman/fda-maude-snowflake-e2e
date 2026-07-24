# MAUDE Ingestion + Clinician Cortex Agents - Design

Ingest the full FDA MAUDE device-adverse-event dataset into Snowflake via a native medallion pipeline (openFDA bulk JSON backfill + weekly incremental), profile the data, and stand up clinician-facing Cortex Agents for device safety intelligence.

- Target: `MAUDE_DB` (schemas `RAW`, `CURATED`, `ANALYTICS`), warehouse `MAUDE_WH`
- Scope: full historical (2009+), ongoing pipeline
- Source: openFDA bulk JSON (recommended), hybrid backfill + weekly incremental

---

## 1. Dataset facts

Verified against the live openFDA manifest (`https://api.fda.gov/download.json`, export 2026-07-22):

| Attribute | Value |
|---|---|
| Endpoint | `device/event` (MAUDE core) |
| Records | 25,368,161 |
| Partition files | 362, partitioned by quarter (`device/event/2004q3/device-event-0001-of-0001.json.zip`) |
| Compressed size | ~17.6 GB (zipped JSON) |
| Refresh cadence | Weekly |
| License | CC0 / public domain |

Each record is a **pre-joined MDR** (Medical Device Report): master event + `device[]` + `patient[]` + `mdr_text[]` (narrative), plus an `openfda` annotation block (`product_code`, `medical_specialty_description`, `device_class`, `regulation_number`, k-number). That annotation is the key advantage of the JSON path - it gives clinician-relevant device classification without extra joins.

Recent quarters get re-published as late/supplemental reports arrive, so incremental must **re-load changed partitions**, not just append new ones.

### Governance notes
- FOIA redactions appear inline as `(b)(4)` (trade secret) and `(b)(6)` (patient/personnel).
- FDA disclaimer: MAUDE is passive surveillance and must **not** drive individual patient-care decisions. It cannot establish event rates or causation. Use is limited to device-level postmarket surveillance. Agents must surface this.

---

## 2. Source path recommendation

**Recommended: hybrid on openFDA bulk JSON.** Backfill from the 362 quarterly zips; incremental via weekly manifest diff.

| Option | Verdict | Reason |
|---|---|---|
| openFDA bulk JSON | **Chosen** | Pre-joined event+device+patient+text, plus `openfda` device classification. One VARIANT row per MDR. |
| Raw pipe-delimited files | Rejected | Requires reconstructing a 6-way join across `mdrfoi`/`foidev`/`patient`/`foitext`/problem-code files; Latin-1 encoding; embedded `\|` in narratives; no `openfda` enrichment. |
| Pure REST API | Rejected | `skip` caps at 26k rows and rate limits make a 25.4M-row backfill impractical. Bulk zips are the sanctioned full-history path. |

---

## 3. Architecture

```mermaid
flowchart LR
  subgraph src [openFDA]
    manifest["download.json manifest"]
    zips["362 quarterly JSON zips (~17.6GB)"]
  end
  subgraph raw [MAUDE_DB.RAW - Bronze]
    stg["@MAUDE_STAGE internal stage"]
    rawtbl["RAW_DEVICE_EVENT (VARIANT)"]
    ctrl["LOAD_CONTROL + MANIFEST_HISTORY"]
  end
  subgraph cur [MAUDE_DB.CURATED - Silver Dynamic Tables]
    fact["FACT_ADVERSE_EVENT"]
    dimdev["DIM_DEVICE"]
    narr["EVENT_NARRATIVE"]
    prob["BRIDGE_DEVICE_PROBLEM / PATIENT_PROBLEM"]
    pat["PATIENT_OUTCOME"]
  end
  subgraph an [MAUDE_DB.ANALYTICS - Gold]
    sv["Semantic View"]
    css["Cortex Search (narratives)"]
    ai["AI-enriched cols (AI_CLASSIFY / AI_EXTRACT)"]
    agents["Cortex Agents"]
  end
  manifest -->|"weekly diff (EAI)"| ctrl
  zips -->|"Snowpark proc PUT"| stg --> rawtbl
  rawtbl --> fact & dimdev & narr & prob & pat
  fact & dimdev & narr & prob & pat --> sv
  narr --> css
  narr --> ai
  sv & css & ai --> agents
```

---

## 4. Implementation steps

### Step 1 - Provision environment
Create `MAUDE_DB` with schemas `RAW`, `CURATED`, `ANALYTICS`; warehouse `MAUDE_WH` (start MEDIUM for the ~17.6 GB backfill, then a serverless/XS lane for incremental). Create External Access Integration + network rule allowing `download.open.fda.gov` and `api.fda.gov`. Roles: `MAUDE_ENGINEER` (build/load), `MAUDE_CLINICIAN` (read-only ANALYTICS + agent usage).

### Step 2 - RAW (Bronze) layer
- Internal stage `@RAW.MAUDE_STAGE` (directory table on).
- `RAW_DEVICE_EVENT` (VARIANT + `_partition`, `_export_date`, `_loaded_at`, `_src_file`).
- Control tables `LOAD_CONTROL` (partition, record_count, export_date, status, load_ts) and `MANIFEST_HISTORY` (raw manifest snapshot per run).
- JSON files are `{"meta":..,"results":[...]}` - load with `STRIP_OUTER_ARRAY` off and `LATERAL FLATTEN(results)` into one VARIANT row per MDR.

### Step 3 - Backfill loader (Snowpark Python stored proc)
`SP_MAUDE_BACKFILL()`: read manifest, iterate the 362 event partitions, stream each zip via EAI, `PUT` to stage, `COPY INTO RAW_DEVICE_EVENT` flattening `results`, upsert `LOAD_CONTROL`. Idempotent per partition (delete-by-`_partition` then reload) so reruns are safe. Batch/loop to bound memory; run once for history.

### Step 4 - Incremental pipeline
`SP_MAUDE_SYNC()`: pull manifest, diff `export_date` + per-partition `records` vs `LOAD_CONTROL`, reload only changed/new quarter partitions (idempotent), snapshot manifest. Wrap in serverless `TASK_MAUDE_WEEKLY` (weekly schedule). Downstream layers refresh automatically via Dynamic Table `TARGET_LAG` and Cortex Search lag - no manual reprocessing.

### Step 5 - CURATED (Silver) star schema as Dynamic Tables
Incremental refresh, `TARGET_LAG='24 hours'`, grain reconstructed from the VARIANT:

- **`FACT_ADVERSE_EVENT`** (grain = `mdr_report_key`): event_type, date_received, date_of_event, report_source_code, reporter_occupation, adverse_event_flag, product_problem_flag.
- **`DIM_DEVICE`** (flatten `device[]`): brand_name, generic_name, manufacturer_d_name, model, product_code, `openfda.device_class`, `openfda.medical_specialty_description`, `openfda.regulation_number`.
- **`EVENT_NARRATIVE`** (flatten `mdr_text[]`): mdr_report_key, text_type_code, `text` (clinical narrative) + `redaction_flag` derived from `(b)(4)`/`(b)(6)` presence.
- **`PATIENT_OUTCOME`** (flatten `patient[]`) and **`BRIDGE_DEVICE_PROBLEM`** / **`BRIDGE_PATIENT_PROBLEM`** (problem codes).

Enforce dedup on `mdr_report_key` (keep latest `_export_date`) to collapse supplemental report chains.

### Step 6 - Data profiling
Snowflake Notebook `01_maude_profiling.ipynb` + companion SQL producing:
- Total rows and rows/quarter growth trend.
- Null-rate per key field (event_type, date_received, product_code, brand_name, manufacturer, patient outcome).
- Cardinality (distinct product_code, brand, manufacturer, medical_specialty).
- Event_type distribution (Death / Injury / Malfunction), device_class, report_source.
- Narrative length distribution + % usable + **redaction rate**.
- Date coverage and reporting lag (`date_of_event` -> `date_received`).
- `mdr_report_key` uniqueness.

Attach DMFs (freshness, row_count, null_count, unique) on `FACT_ADVERSE_EVENT` for ongoing monitoring. Use `visualize_data` for the key distributions.

### Step 7 - ANALYTICS (Gold) layer
- **Semantic view** over the star schema (facts: report counts by event_type/year; dims: device, manufacturer, product_code, medical_specialty, phase; verified queries for common safety questions) - powers Cortex Analyst.
- **Cortex Search service** over `EVENT_NARRATIVE.text` (attributes: mdr_report_key, product_code, brand_name, event_type, date_received) with `TARGET_LAG` so new reports auto-index.
- **AI enrichment** (incremental DT over new narratives to bound cost): `AI_CLASSIFY` for failure-mode / severity taxonomy, `AI_EXTRACT` for structured signals (component involved, use error vs device failure). Gate with Cortex Code cost controls.

### Step 8 - Clinician Cortex Agents
Frame = device safety intelligence, not patient care. Build the top 2 first, then optionally 3-4.

1. **Device Safety Profile Agent** (build first): Cortex Analyst (semantic view) + Cortex Search (narratives, cited by `mdr_report_key`) + `data_to_chart` for trend/comparison visuals. Example: "What malfunctions have been reported for the [X] valve/pump in the last 3 years, and what patient outcomes were noted?"
2. **Failure-Mode Narrative Search Agent** (build first): Cortex Search-led RAG over 25M narratives. Example: "Find reports describing catheter tip fracture during retrieval." Returns cited MDRs.
3. **Comparative Device Risk Agent** (optional): compare event-type/problem-code distribution across devices sharing a product_code (procurement/formulary).
4. **Emerging Signal / Trend Agent** (optional): time-series spike detection by product_code/problem_code for safety officers and clinical engineering.

As-built, both agents ship with starter `sample_questions` and pin `MAUDE_WH` via each tool's `execution_environment`. Citations use `citation_title` (composite: "brand - event_type, year"), include `report_number` and a clickable `source_url` (FDA MAUDE detail page deep link keyed on `mdr_report_key`). Response instructions tell the agent to surface these in every cited report. Every agent's response template must include the FDA disclaimer (MAUDE cannot establish rates/causation; not for individual care decisions) and cite MDR keys. RBAC: `MAUDE_CLINICIAN` gets read-only on ANALYTICS + agent usage only.

---

## 5. Parallel backfill (as-built)

The initial serial backfill (one proc looping 362 partitions) is slow because each
partition's HTTP download + `ijson` parse runs single-node in Python - a larger
warehouse cannot speed a single lane. Backfill is therefore fanned out across
**~43 disjoint time-bucket serverless task lanes** (`TASK_MAUDE_BF_*`), each an
XSMALL task calling `SP_MAUDE_INGEST('BACKFILL', 0, '<regex>')`. Older/lighter years
are grouped; the heavy recent years (2019+) are split **per quarter** so the largest
lane is ~1 quarter (~6 files) rather than ~1 year (~24) - this flattens the tail.

- Parallelism comes from many cheap XSMALL lanes, not big warehouses.
- `QUARTER_LIKE` accepts a regex with alternation, e.g. `(2005|2006|2007|2008)q%`.
- Lanes never collide: `BACKFILL` skips already-`LOADED` partitions (idempotent).
- Snowpark async (`AsyncJob`) was rejected - it parallelizes SQL submission, not
  the Python download bottleneck.
- Teardown: `DROP` the `TASK_MAUDE_BF_*` lanes once the backfill completes.

**Measured (first run, 25.37M rows / 362 partitions):** the 18-lane year-level
version loaded the full history in **~43 min** wall-clock (peak 18-22 partitions/min)
vs an extrapolated **~6 h** serial (~1 partition/min) - roughly **8x**. The tail was
bound by the biggest single-year lane, which is why the checked-in version now splits
heavy years by quarter.

---

## 6. Verification

- **Load integrity:** `COUNT(*) FROM RAW.RAW_DEVICE_EVENT` reconciles to
  `SUM(loaded_records)` in `LOAD_CONTROL` and to the manifest `total_records`
  (~25.37M as of 2026-07-22, within weekly refresh drift).
- **Grain:** `FACT_ADVERSE_EVENT` has no duplicate `mdr_report_key`; child flatten
  counts (device/narrative/patient/problem) are >= fact rows. Validated on the
  2010q1 partition: 64,517 rows loaded == manifest count.
- **Dynamic Tables:** show current numbers only after they refresh (TARGET_LAG
  1 day); force with `ALTER DYNAMIC TABLE ... REFRESH` or let them settle after
  backfill.
- **Incremental:** run `SP_MAUDE_INGEST('SYNC',0,NULL)` twice - the second run
  loads 0 partitions (no false diffs).
- **DMFs** attached on `FACT_ADVERSE_EVENT` (row count, duplicate, null) evaluate
  on the daily schedule.
- **Cortex Search** returns cited MDRs (validated: "trocar seal leaking" -> Endopath
  trocar malfunction reports). **Cortex Analyst** answers event-type counts.
- **RBAC:** `MAUDE_CLINICIAN` reads ANALYTICS + agents only, not RAW/CURATED.

---

## 7. Files (as-built) and runbook

Run in order as `MAUDE_ENGINEER` (00 needs ACCOUNTADMIN for the EAI / task grants):

| File | Purpose |
|---|---|
| `sql/00_setup.sql` | DB/schemas/warehouse, roles, EAI + network rule, task-exec grants |
| `sql/01_raw.sql` | stage, JSON file format, `RAW_DEVICE_EVENT`, `LOAD_CONTROL`, `MANIFEST_HISTORY` |
| `sql/02_ingest.sql` | `SP_MAUDE_INGEST` (backfill+sync), `TASK_MAUDE_WEEKLY_SYNC`, `TASK_MAUDE_BACKFILL` |
| `sql/03_curated.sql` | star schema Dynamic Tables (fact/dims/narrative/patient/problem bridge) |
| `sql/04_analytics.sql` | `V_NARRATIVE_ENRICHED`, semantic view `MAUDE_SAFETY_SV`, Cortex Search `MAUDE_NARRATIVE_SEARCH`, `AI_EVENT_ENRICHMENT`, clinician grants |
| `sql/05_agents.sql` | `MAUDE_DEVICE_SAFETY_AGENT` + `MAUDE_FAILURE_MODE_AGENT` + grants |
| `sql/06_profiling.sql` | profiling queries + DMFs |
| `sql/07_backfill_fanout.sql` | parallel 18-lane backfill launcher + teardown |
| `notebooks/01_maude_profiling.ipynb` | Snowsight profiling notebook (SQL cells: backfill status, volume, quality, narratives, redaction) |
| `diagrams/pipeline_architecture.excalidraw` | architecture diagram |

The notebook is deployed as Snowsight notebook `MAUDE_DB.ANALYTICS.MAUDE_PROFILING`
(staged in `@MAUDE_DB.ANALYTICS.NOTEBOOKS`, warehouse `MAUDE_WH`). Redeploy after
edits with: `PUT file://.../01_maude_profiling.ipynb @MAUDE_DB.ANALYTICS.NOTEBOOKS/maude_profiling OVERWRITE=TRUE`
then `ALTER NOTEBOOK MAUDE_DB.ANALYTICS.MAUDE_PROFILING ADD LIVE VERSION FROM LAST;`

**Backfill options:** single async task (`EXECUTE TASK TASK_MAUDE_BACKFILL`) or, for
speed, the parallel lanes in `07_backfill_fanout.sql`. **After backfill:** resume
ongoing sync with `ALTER TASK MAUDE_DB.RAW.TASK_MAUDE_WEEKLY_SYNC RESUME;` and drop
the `TASK_MAUDE_BF_*` lanes.
