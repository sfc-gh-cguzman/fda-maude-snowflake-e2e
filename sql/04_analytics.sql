-- =====================================================================
-- MAUDE Ingestion - 04_analytics.sql
-- Gold layer: semantic view (Cortex Analyst), Cortex Search service over
-- narratives, and a cost-bounded AI enrichment table.
--
-- Run as MAUDE_ENGINEER. Idempotent.
-- =====================================================================

USE ROLE MAUDE_ENGINEER;
USE WAREHOUSE MAUDE_WH;
USE DATABASE MAUDE_DB;
USE SCHEMA ANALYTICS;

-- ---------------------------------------------------------------------
-- Device-deduped narrative corpus view: one clean (report, narrative)
-- row joined to its primary device + event facts. Avoids device fanout
-- inflating the search/agent corpus.
-- ---------------------------------------------------------------------
CREATE OR REPLACE VIEW V_NARRATIVE_ENRICHED AS
SELECT
    n.mdr_report_key,
    n.text_type_code,
    n.narrative_text,
    n.narrative_length,
    n.redaction_flag,
    e.event_type,
    e.date_received,
    e.report_year,
    e.reporter_occupation_code,
    d.brand_name,
    d.generic_name,
    d.device_manufacturer_name,
    d.product_code,
    d.device_class,
    d.medical_specialty
FROM MAUDE_DB.CURATED.EVENT_NARRATIVE n
JOIN MAUDE_DB.CURATED.FACT_ADVERSE_EVENT e USING (mdr_report_key)
LEFT JOIN (
    SELECT mdr_report_key, brand_name, generic_name, device_manufacturer_name,
           product_code, device_class, medical_specialty,
           ROW_NUMBER() OVER (PARTITION BY mdr_report_key ORDER BY device_sequence_number) AS rn
    FROM MAUDE_DB.CURATED.DIM_DEVICE
    QUALIFY rn = 1
) d USING (mdr_report_key);

-- ---------------------------------------------------------------------
-- MDR-grain dimension views for the semantic model.
--
-- DIM_DEVICE and PATIENT_OUTCOME are landed at their source grain, which
-- is *below* one-row-per-MDR (a report can name several devices). A
-- semantic view can only slice a metric by dimensions on the "one" side
-- of a relationship, so wiring DIM_DEVICE directly as a child of EVENTS
-- makes brand / product code / manufacturer unusable with any event
-- metric ("higher level of granularity than the base metric entity").
--
-- Measured on the 2021+ load (14,563,790 MDRs): 17,651 reports name more than
-- one device row, but only 1,024 name more than one distinct brand and 412 more
-- than one manufacturer - the rest are same-vendor component sets (pump +
-- reservoir cassettes, sensor + reader). Real information loss from collapsing
-- is therefore ~0.007%, and the top hidden secondary manufacturers are
-- 'UNK'/'UNKNOWN' (177 of 412). PATIENT_OUTCOME is 1.0000 rows per MDR.
--
-- Known tradeoff, accepted deliberately: cross-vendor device *interaction* is
-- invisible here (e.g. SoClean ozone cleaners co-reported with Philips
-- Respironics CPAP units). Answer those with a direct query against
-- CURATED.DIM_DEVICE, which retains every device row.
-- ---------------------------------------------------------------------
CREATE OR REPLACE VIEW MAUDE_DB.CURATED.V_DEVICE_PRIMARY
COMMENT = 'Primary (lowest device_sequence_number) device per MDR - one row per report'
AS
SELECT mdr_report_key, brand_name, generic_name, device_manufacturer_name,
       product_code, device_class, medical_specialty
FROM MAUDE_DB.CURATED.DIM_DEVICE
QUALIFY ROW_NUMBER() OVER (
          PARTITION BY mdr_report_key
          ORDER BY device_sequence_number, brand_name, product_code) = 1;

-- patient_sex arrives with three distinct spellings of "unknown" ('Unknown',
-- empty string, NULL). Collapsing the empty string keeps Cortex Analyst from
-- reporting 1.4M events under a blank label as if it were its own cohort.
CREATE OR REPLACE VIEW MAUDE_DB.CURATED.V_PATIENT_PRIMARY
COMMENT = 'One patient-outcome row per MDR; blank patient_sex normalized to Unknown'
AS
SELECT mdr_report_key,
       COALESCE(NULLIF(patient_sex, ''), 'Unknown') AS patient_sex
FROM MAUDE_DB.CURATED.PATIENT_OUTCOME
QUALIFY ROW_NUMBER() OVER (
          PARTITION BY mdr_report_key
          ORDER BY NULLIF(patient_sex, '') NULLS LAST) = 1;

-- ---------------------------------------------------------------------
-- Semantic view for Cortex Analyst (structured Q&A).
--
-- Relationship direction matters: EVENTS is the CHILD pointing up at the
-- MDR-grain device / patient parents, so event metrics can be sliced by
-- brand, product code, manufacturer, and specialty without fanout.
-- PROBLEMS stays a child of EVENTS (2.19 rows per MDR - genuinely
-- many-to-one) and therefore carries its own COUNT(DISTINCT) metric
-- instead of borrowing the event-grain ones.
-- ---------------------------------------------------------------------
CREATE OR REPLACE SEMANTIC VIEW MAUDE_DB.ANALYTICS.MAUDE_SAFETY_SV
  TABLES (
    events   AS MAUDE_DB.CURATED.FACT_ADVERSE_EVENT PRIMARY KEY (mdr_report_key)
             COMMENT = 'One row per FDA medical device report (MDR)',
    devices  AS MAUDE_DB.CURATED.V_DEVICE_PRIMARY PRIMARY KEY (mdr_report_key)
             COMMENT = 'Primary device named on each report (one row per MDR)',
    patients AS MAUDE_DB.CURATED.V_PATIENT_PRIMARY PRIMARY KEY (mdr_report_key)
             COMMENT = 'Patient outcome per report (one row per MDR)',
    problems AS MAUDE_DB.CURATED.BRIDGE_DEVICE_PROBLEM
             COMMENT = 'Product problem descriptions per report (many per MDR)'
  )
  RELATIONSHIPS (
    evt_to_dev  AS events   (mdr_report_key) REFERENCES devices  (mdr_report_key),
    evt_to_pat  AS events   (mdr_report_key) REFERENCES patients (mdr_report_key),
    prob_to_evt AS problems (mdr_report_key) REFERENCES events   (mdr_report_key)
  )
  DIMENSIONS (
    events.event_type        AS event_type        COMMENT = 'Death | Injury | Malfunction | Other',
    events.report_year       AS report_year       COMMENT = 'Year FDA received the report',
    events.report_source_code AS report_source_code,
    events.reporter_occupation AS reporter_occupation_code,
    events.date_received     AS date_received,
    devices.brand_name       AS brand_name        COMMENT = 'Device brand / trade name',
    devices.generic_name     AS generic_name,
    devices.product_code     AS product_code       COMMENT = 'FDA device product code',
    devices.device_class     AS device_class       COMMENT = 'FDA device class 1/2/3',
    devices.medical_specialty AS medical_specialty  COMMENT = 'FDA medical specialty panel',
    devices.manufacturer_name AS device_manufacturer_name
             COMMENT = 'Device manufacturer as reported on the device record',
    problems.device_problem  AS device_problem     COMMENT = 'FDA product problem description',
    patients.patient_sex     AS patient_sex
  )
  METRICS (
    events.report_count       AS COUNT(events.mdr_report_key) COMMENT = 'Number of MDRs',
    events.death_count        AS COUNT(CASE WHEN events.event_type = 'Death' THEN 1 END) COMMENT = 'MDRs coded as Death',
    events.injury_count       AS COUNT(CASE WHEN events.event_type = 'Injury' THEN 1 END) COMMENT = 'MDRs coded as Injury',
    events.malfunction_count  AS COUNT(CASE WHEN events.event_type = 'Malfunction' THEN 1 END) COMMENT = 'MDRs coded as Malfunction',
    events.avg_reporting_lag  AS AVG(CASE WHEN events.reporting_lag_days BETWEEN 0 AND 3650
                                          THEN events.reporting_lag_days END)
             COMMENT = 'Avg days from event to FDA receipt. Excludes impossible/implausible lags (raw data ranges -35396 to 45847 days) and the ~10% of MDRs with no event date. Distribution is right-skewed (median ~27d), so treat the mean as indicative only',
    problems.affected_report_count AS COUNT(DISTINCT problems.mdr_report_key)
             COMMENT = 'Distinct MDRs citing a given product problem (use with device_problem). Do NOT sum across device_problem values - an MDR citing several problems is counted in each group'
  )
  COMMENT = 'FDA MAUDE device adverse event semantic model. Postmarket surveillance only - not for individual patient care decisions; cannot establish rates or causation.';

-- ---------------------------------------------------------------------
-- Cortex Search service over MDR narratives (unstructured Q&A / RAG).
-- Auto-reindexes as new reports land (TARGET_LAG). Query is inlined over
-- the Dynamic Tables directly: a Cortex Search service cannot read from a
-- view that references Dynamic Tables.
-- ---------------------------------------------------------------------
CREATE OR REPLACE CORTEX SEARCH SERVICE MAUDE_DB.ANALYTICS.MAUDE_NARRATIVE_SEARCH
  ON narrative_text
  ATTRIBUTES mdr_report_key, report_number, text_type_code, event_type, product_code,
             brand_name, medical_specialty, device_class, report_year, redaction_flag,
             citation_title, source_url
  WAREHOUSE = MAUDE_WH
  TARGET_LAG = '1 day'
  COMMENT = 'Semantic search over FDA MAUDE adverse-event narratives, cited by MDR key with FDA deep link'
  AS
  SELECT
      n.mdr_report_key, n.narrative_text, n.text_type_code, n.redaction_flag,
      e.event_type, e.report_year, e.report_number,
      d.product_code, d.brand_name, d.medical_specialty, d.device_class,
      -- richer, human-readable citation label
      COALESCE(d.brand_name, '(unknown device)') || ' - '
        || COALESCE(e.event_type, '?') || ', ' || COALESCE(e.report_year::string, '?') AS citation_title,
      -- click-through link to the public openFDA API record (cfMAUDE detail has gaps for older records)
      'https://api.fda.gov/device/event.json?search=mdr_report_key:' || n.mdr_report_key || '&limit=1' AS source_url
  FROM MAUDE_DB.CURATED.EVENT_NARRATIVE n
  JOIN MAUDE_DB.CURATED.FACT_ADVERSE_EVENT e USING (mdr_report_key)
  LEFT JOIN (
      SELECT mdr_report_key, brand_name, product_code, device_class, medical_specialty,
             ROW_NUMBER() OVER (PARTITION BY mdr_report_key ORDER BY device_sequence_number) AS rn
      FROM MAUDE_DB.CURATED.DIM_DEVICE QUALIFY rn = 1
  ) d USING (mdr_report_key)
  WHERE n.narrative_text IS NOT NULL AND n.narrative_length >= 20;

-- ---------------------------------------------------------------------
-- AI enrichment (cost-bounded). Classifies the reporter narrative into a
-- clinical failure-mode + severity taxonomy. Materialized on a SAMPLE for
-- build validation; scale via the documented INSERT + serverless task,
-- gated by Cortex Code cost controls.
-- ---------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS AI_EVENT_ENRICHMENT (
    mdr_report_key STRING,
    failure_mode   STRING,
    severity_bucket STRING,
    enriched_at    TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
)
COMMENT = 'AI_CLASSIFY-derived failure mode + severity per MDR (cost-bounded)';

-- Build/validate on a bounded sample (safe to re-run for the demo).
INSERT OVERWRITE INTO AI_EVENT_ENRICHMENT (mdr_report_key, failure_mode, severity_bucket)
SELECT
    mdr_report_key,
    AI_CLASSIFY(narrative_text,
        ['Device Malfunction','Use Error','Material or Component Failure',
         'Software or Connectivity','Packaging or Sterility','Patient Injury',
         'No Clear Failure']):labels[0]::string          AS failure_mode,
    AI_CLASSIFY(narrative_text,
        ['Death','Serious Injury','Malfunction Only','Unclear']):labels[0]::string AS severity_bucket
FROM MAUDE_DB.ANALYTICS.V_NARRATIVE_ENRICHED
WHERE text_type_code = 'Description of Event or Problem'
  AND narrative_length BETWEEN 40 AND 4000
LIMIT 200;

-- Scale-up pattern (run under a serverless task with a cost budget):
--   INSERT INTO AI_EVENT_ENRICHMENT ...
--   SELECT ... FROM V_NARRATIVE_ENRICHED v
--   WHERE v.text_type_code = 'Description of Event or Problem'
--     AND v.mdr_report_key NOT IN (SELECT mdr_report_key FROM AI_EVENT_ENRICHMENT);

-- ---------------------------------------------------------------------
-- Clinician read access to the gold objects.
-- ---------------------------------------------------------------------
GRANT SELECT ON VIEW  MAUDE_DB.ANALYTICS.V_NARRATIVE_ENRICHED TO ROLE MAUDE_CLINICIAN;
GRANT SELECT ON TABLE MAUDE_DB.ANALYTICS.AI_EVENT_ENRICHMENT  TO ROLE MAUDE_CLINICIAN;
GRANT USAGE  ON CORTEX SEARCH SERVICE MAUDE_DB.ANALYTICS.MAUDE_NARRATIVE_SEARCH TO ROLE MAUDE_CLINICIAN;
GRANT SELECT ON SEMANTIC VIEW MAUDE_DB.ANALYTICS.MAUDE_SAFETY_SV TO ROLE MAUDE_CLINICIAN;
