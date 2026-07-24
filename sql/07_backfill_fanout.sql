-- =====================================================================
-- MAUDE Ingestion - 07_backfill_fanout.sql
-- Parallel backfill via disjoint task lanes (bucketed by time period).
--
-- Rationale: SP_MAUDE_INGEST is single-node Python (HTTP download +
-- ijson parse), so a larger warehouse does NOT speed a single lane.
-- Parallelism comes from MANY lanes on XSMALL - cheap and effective.
-- Each lane runs BACKFILL with a disjoint QUARTER_LIKE regex; the proc
-- is idempotent and skips already-LOADED partitions, so lanes never
-- collide and reruns are safe.
--
-- Bucketing (measured lesson from the first run): tail latency is bound
-- by the single biggest lane running serially within itself. The heavy
-- recent years (2019+) are therefore split PER QUARTER, not per year, so
-- the largest lane is ~1 quarter (~6 files) instead of ~1 year (~24).
-- Lighter/older years stay grouped. This launches ~43 lanes; if you hit
-- serverless-task concurrency limits, coarsen the recent-year buckets
-- back toward year-level (e.g. '2019q%').
--
-- Run as MAUDE_ENGINEER (needs EXECUTE MANAGED TASK - see 00_setup.sql).
-- =====================================================================

USE ROLE MAUDE_ENGINEER;
USE WAREHOUSE MAUDE_WH;
USE DATABASE MAUDE_DB;
USE SCHEMA RAW;

-- Launch all lanes: creates one XSMALL serverless task per bucket and
-- fires it immediately. QUARTER_LIKE accepts a regex (alternation).
DECLARE
  buckets ARRAY := ARRAY_CONSTRUCT(
    -- older / lighter years: grouped
    '(1991|1992|1993|1994|1995|1996|1997|1998|1999|2000|2001|2002|2003|2004)q%',
    '(2005|2006|2007|2008)q%',
    '(2009|2010)q%',
    '(2011|2012)q%',
    '2013q%', '2014q%', '2015q%', '2016q%', '2017q%', '2018q%',
    -- heavy recent years: split PER QUARTER to flatten the tail
    '2019q1%', '2019q2%', '2019q3%', '2019q4%',
    '2020q1%', '2020q2%', '2020q3%', '2020q4%',
    '2021q1%', '2021q2%', '2021q3%', '2021q4%',
    '2022q1%', '2022q2%', '2022q3%', '2022q4%',
    '2023q1%', '2023q2%', '2023q3%', '2023q4%',
    '2024q1%', '2024q2%', '2024q3%', '2024q4%',
    '2025q1%', '2025q2%', '2025q3%', '2025q4%',
    '2026q1%', '2026q2%', '2026q3%', '2026q4%',
    'all_other%'   -- openFDA bucket for records without a valid quarter
  );
  i INT;
  b STRING;
BEGIN
  FOR i IN 0 TO ARRAY_SIZE(buckets) - 1 DO
    b := buckets[i]::string;
    EXECUTE IMMEDIATE
      'CREATE OR REPLACE TASK MAUDE_DB.RAW.TASK_MAUDE_BF_' || i ||
      ' USER_TASK_MANAGED_INITIAL_WAREHOUSE_SIZE = ''XSMALL''' ||
      ' USER_TASK_TIMEOUT_MS = 86400000' ||
      ' SUSPEND_TASK_AFTER_NUM_FAILURES = 0' ||
      ' AS CALL MAUDE_DB.RAW.SP_MAUDE_INGEST(''BACKFILL'', 0, ''' || b || ''')';
    EXECUTE IMMEDIATE 'EXECUTE TASK MAUDE_DB.RAW.TASK_MAUDE_BF_' || i;
  END FOR;
  RETURN 'launched ' || ARRAY_SIZE(buckets) || ' backfill lanes';
END;

-- Monitor:
--   SELECT status, COUNT(*) partitions, SUM(loaded_records) rows
--   FROM MAUDE_DB.RAW.LOAD_CONTROL GROUP BY status;
--   SELECT name, state FROM TABLE(INFORMATION_SCHEMA.TASK_HISTORY())
--    WHERE name ILIKE 'TASK_MAUDE_BF_%' ORDER BY scheduled_time DESC;

-- Teardown after backfill completes (drop the transient lanes).
-- The loop upper bound must be >= number of buckets (currently 43).
--   DECLARE i INT; BEGIN FOR i IN 0 TO 42 DO
--     EXECUTE IMMEDIATE 'DROP TASK IF EXISTS MAUDE_DB.RAW.TASK_MAUDE_BF_'||i;
--   END FOR; END;
