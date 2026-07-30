---
name: maude-deploy
description: Deploy the FDA MAUDE adverse-event pipeline + clinician Cortex Agents in any Snowflake account. Full medallion (RAW/CURATED/ANALYTICS), parallel backfill, semantic view, Cortex Search over 58M narratives, two agents with FDA source-URL citations.
---

# Deploy FDA MAUDE Pipeline

Deploys a Snowflake-native pipeline that ingests the full FDA MAUDE device adverse-event
dataset, curates it into a star schema, and exposes it through Cortex Agents for device-safety
intelligence. All data comes from the openFDA public API (CC0, no license/agreement needed).

## Prerequisites

- Active Snowflake connection
- A role with CREATE DATABASE, CREATE WAREHOUSE, CREATE INTEGRATION privileges (typically SYSADMIN + ACCOUNTADMIN for the EAI)
- The account must support External Access Integrations (standard in all commercial regions)

## Step 1: Configuration

Ask the user for:

1. **Target database** (default: `MAUDE_DB`)
2. **Target warehouse** (default: `MAUDE_WH`, created as MEDIUM)
3. **Role** with CREATE privileges (default: `SYSADMIN`)
4. **Data scope** - how much history to load:

| Scope | Records | Partitions | Backfill | Search index | Total wall-clock |
|---|---|---|---|---|---|
| **Full history (1993-2026)** | ~25.4M | 362 | ~45 min | ~30 min | ~75 min |
| **Last 10 years (2016+)** | ~16M | ~180 | ~20 min | ~15 min | ~35 min |
| **Last 5 years (2021+)** | ~10M | ~100 | ~12 min | ~10 min | ~22 min |

All options use parallel fan-out (many XSMALL serverless task lanes). Backfill is idempotent
and can be re-run if interrupted. Weekly incremental sync keeps data fresh regardless of scope.

Present the table and ask the user to choose. Default to "Last 10 years" if unsure.

## Step 2: Provision environment

Execute `scripts/00_setup.sql` statements (adapt DB/warehouse/role names per Step 1 answers):

- CREATE DATABASE, schemas RAW/CURATED/ANALYTICS
- CREATE WAREHOUSE (MEDIUM, auto-suspend 60s)
- CREATE ROLE MAUDE_ENGINEER + MAUDE_CLINICIAN with grants
- CREATE NETWORK RULE + EXTERNAL ACCESS INTEGRATION (needs ACCOUNTADMIN)
- GRANT EXECUTE MANAGED TASK ON ACCOUNT TO ROLE MAUDE_ENGINEER

Then execute `scripts/01_raw.sql`:

- CREATE STAGE, FILE FORMAT, RAW_DEVICE_EVENT table, LOAD_CONTROL, MANIFEST_HISTORY

**Verify:** `SHOW SCHEMAS IN DATABASE <DB>` returns RAW, CURATED, ANALYTICS.

## Step 3: Create ingest pipeline

Execute `scripts/02_ingest.sql`:

- CREATE PROCEDURE SP_MAUDE_INGEST (Snowpark Python, uses EAI + ijson for stream parsing)
- CREATE TASK TASK_MAUDE_WEEKLY_SYNC (suspended)
- CREATE TASK TASK_MAUDE_BACKFILL (one-off, for reference)

**Verify:** `SHOW PROCEDURES LIKE 'SP_MAUDE_INGEST' IN SCHEMA <DB>.RAW` returns 1 row.

## Step 4: Kick off parallel backfill

Generate and execute the fan-out launcher. The DECLARE block creates XSMALL serverless task
lanes, each calling SP_MAUDE_INGEST with a disjoint QUARTER_LIKE regex. Scope the bucket
list based on the user's Step 1 choice:

### Full history (1993-2026) — all buckets:
```sql
DECLARE
  buckets ARRAY := ARRAY_CONSTRUCT(
    '(1991|1992|1993|1994|1995|1996|1997|1998|1999|2000|2001|2002|2003|2004)q%',
    '(2005|2006|2007|2008)q%',
    '(2009|2010)q%',
    '(2011|2012)q%',
    '2013q%', '2014q%', '2015q%', '2016q%', '2017q%', '2018q%',
    '2019q1%', '2019q2%', '2019q3%', '2019q4%',
    '2020q1%', '2020q2%', '2020q3%', '2020q4%',
    '2021q1%', '2021q2%', '2021q3%', '2021q4%',
    '2022q1%', '2022q2%', '2022q3%', '2022q4%',
    '2023q1%', '2023q2%', '2023q3%', '2023q4%',
    '2024q1%', '2024q2%', '2024q3%', '2024q4%',
    '2025q1%', '2025q2%', '2025q3%', '2025q4%',
    '2026q1%', '2026q2%', '2026q3%', '2026q4%',
    'all_other%'
  );
```

### Last 10 years (2016+):
```sql
DECLARE
  buckets ARRAY := ARRAY_CONSTRUCT(
    '(2016|2017|2018)q%',
    '2019q1%', '2019q2%', '2019q3%', '2019q4%',
    '2020q1%', '2020q2%', '2020q3%', '2020q4%',
    '2021q1%', '2021q2%', '2021q3%', '2021q4%',
    '2022q1%', '2022q2%', '2022q3%', '2022q4%',
    '2023q1%', '2023q2%', '2023q3%', '2023q4%',
    '2024q1%', '2024q2%', '2024q3%', '2024q4%',
    '2025q1%', '2025q2%', '2025q3%', '2025q4%',
    '2026q1%', '2026q2%', '2026q3%', '2026q4%',
    'all_other%'
  );
```

### Last 5 years (2021+):
```sql
DECLARE
  buckets ARRAY := ARRAY_CONSTRUCT(
    '2021q1%', '2021q2%', '2021q3%', '2021q4%',
    '2022q1%', '2022q2%', '2022q3%', '2022q4%',
    '2023q1%', '2023q2%', '2023q3%', '2023q4%',
    '2024q1%', '2024q2%', '2024q3%', '2024q4%',
    '2025q1%', '2025q2%', '2025q3%', '2025q4%',
    '2026q1%', '2026q2%', '2026q3%', '2026q4%',
    'all_other%'
  );
```

After the buckets declaration, the loop body is identical for all scopes:
```sql
  i INT;
  b STRING;
BEGIN
  FOR i IN 0 TO ARRAY_SIZE(buckets) - 1 DO
    b := buckets[i]::string;
    EXECUTE IMMEDIATE
      'CREATE OR REPLACE TASK <DB>.RAW.TASK_MAUDE_BF_' || i ||
      ' USER_TASK_MANAGED_INITIAL_WAREHOUSE_SIZE = ''XSMALL''' ||
      ' USER_TASK_TIMEOUT_MS = 86400000' ||
      ' SUSPEND_TASK_AFTER_NUM_FAILURES = 0' ||
      ' AS CALL <DB>.RAW.SP_MAUDE_INGEST(''BACKFILL'', 0, ''' || b || ''')';
    EXECUTE IMMEDIATE 'EXECUTE TASK <DB>.RAW.TASK_MAUDE_BF_' || i;
  END FOR;
  RETURN 'launched ' || ARRAY_SIZE(buckets) || ' backfill lanes';
END;
```

Replace `<DB>` with the target database name from Step 1.

**Monitor progress** (show this to the user):
```sql
SELECT status, COUNT(*) AS partitions, SUM(loaded_records) AS rows_loaded
FROM <DB>.RAW.LOAD_CONTROL GROUP BY status;
```

The backfill runs async. Proceed to Step 5 while it loads.

## Step 5: Build CURATED + ANALYTICS + Agents

Execute in order:
1. `scripts/03_curated.sql` — Dynamic Tables (star schema, TARGET_LAG 1 day)
2. `scripts/04_analytics.sql` — Semantic view, Cortex Search service, AI enrichment, grants
3. `scripts/05_agents.sql` — Two Cortex Agents + clinician grants

**Run these with `snow sql --enable-templating NONE`.** The default LEGACY
templating parses the `&limit=1` in the FDA `source_url` expression in
`04_analytics.sql` as a client-side variable and fails with
`SQL template rendering error: 'limit' is undefined` before anything reaches
Snowflake.

**Re-running `04_analytics.sql` rebuilds the Cortex Search service** (it is
`CREATE OR REPLACE`), which re-indexes every narrative from scratch. To change
only the semantic view on an already-indexed deployment, run just the statements
above the Cortex Search block instead of the whole file.

DTs and Search service auto-populate as backfill data lands. No manual action needed.

**Note:** The Cortex Search service indexes asynchronously after creation. It will show
`serving_state: INITIALIZING` until the index build completes (~10-30 min depending on scope).

## Step 6: Verify and finalize

Once `LOAD_CONTROL` shows all partitions LOADED (0 FAILED):

1. **Force-refresh Dynamic Tables** (or wait for the 1-day lag to settle):
```sql
ALTER DYNAMIC TABLE <DB>.CURATED.FACT_ADVERSE_EVENT REFRESH;
ALTER DYNAMIC TABLE <DB>.CURATED.DIM_DEVICE REFRESH;
ALTER DYNAMIC TABLE <DB>.CURATED.EVENT_NARRATIVE REFRESH;
ALTER DYNAMIC TABLE <DB>.CURATED.PATIENT_OUTCOME REFRESH;
ALTER DYNAMIC TABLE <DB>.CURATED.BRIDGE_DEVICE_PROBLEM REFRESH;
```

2. **Confirm search service is active:**
```sql
SHOW CORTEX SEARCH SERVICES LIKE 'MAUDE_NARRATIVE_SEARCH' IN SCHEMA <DB>.ANALYTICS;
-- serving_state should be ACTIVE
```

3. **Resume weekly sync:**
```sql
ALTER TASK <DB>.RAW.TASK_MAUDE_WEEKLY_SYNC RESUME;
```

4. **Drop transient backfill lanes:**
```sql
DECLARE i INT; BEGIN FOR i IN 0 TO 50 DO
  EXECUTE IMMEDIATE 'DROP TASK IF EXISTS <DB>.RAW.TASK_MAUDE_BF_'||i;
END FOR; END;
```

5. **Test the agents:**
- In Snowsight: AI & ML > Agents > MAUDE_DEVICE_SAFETY_AGENT > try a sample question.
- Or via CoCo: `/cortex-agent` > chat > specify `<DB>.ANALYTICS.MAUDE_DEVICE_SAFETY_AGENT`.

## What gets deployed

| Object | Schema | Purpose |
|---|---|---|
| SP_MAUDE_INGEST | RAW | Snowpark ingest proc (backfill + sync) |
| TASK_MAUDE_WEEKLY_SYNC | RAW | Weekly incremental (Mondays 6am PT) |
| RAW_DEVICE_EVENT | RAW | VARIANT landing (1 row/MDR) |
| LOAD_CONTROL | RAW | Per-partition load ledger |
| FACT_ADVERSE_EVENT | CURATED | Master event grain (DT) |
| DIM_DEVICE | CURATED | Device + openFDA classification (DT) |
| EVENT_NARRATIVE | CURATED | ~58M narrative segments + redaction flag (DT) |
| PATIENT_OUTCOME | CURATED | Patient demographics + outcomes (DT) |
| BRIDGE_DEVICE_PROBLEM | CURATED | Product-problem codes (DT) |
| MAUDE_SAFETY_SV | ANALYTICS | Semantic view (Cortex Analyst) |
| MAUDE_NARRATIVE_SEARCH | ANALYTICS | Cortex Search (55M+ narratives, FDA citations) |
| MAUDE_DEVICE_SAFETY_AGENT | ANALYTICS | Structured + unstructured + charts |
| MAUDE_FAILURE_MODE_AGENT | ANALYTICS | Narrative RAG with cited MDRs |

## Credits and cost

- **Backfill (one-time):** many XSMALL serverless lanes for the duration shown in the scope table. Total credits ~2-5 depending on scope.
- **Weekly sync:** XS serverless, seconds of compute per run. Negligible.
- **Dynamic Tables:** refresh on 1-day lag; minimal compute for incremental changes.
- **Cortex Search:** serverless indexing, no warehouse credits.
- **Agents:** per-query Cortex consumption (same as any Cortex Agent).
- **Source data:** free. openFDA is public domain (CC0), no API key required for bulk downloads.

## Governance

Every agent response carries the FDA disclaimer: "MAUDE is a passive surveillance system.
Report counts cannot establish event rates, incidence, or causation, and this information must
not be used for individual patient-care decisions." Citations include `mdr_report_key`,
`report_number`, and a clickable `source_url` to the official FDA MAUDE detail page.

## Reference docs (bundled)

These companion files ship with this skill for deeper context. Read on demand when you need
architecture detail, personas, or sample questions beyond what's in this workflow:

- `references/overview.md` — project README: personas, use cases, sample questions (structured / unstructured / hybrid), architecture diagram, repo layout.
- `references/architecture.md` — full design: dataset facts, source-path rationale, ingestion mechanics, parallel backfill (measured timings), star schema, agents, verification checklist.
