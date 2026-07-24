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
-- Semantic view for Cortex Analyst (structured Q&A).
-- ---------------------------------------------------------------------
CREATE OR REPLACE SEMANTIC VIEW MAUDE_DB.ANALYTICS.MAUDE_SAFETY_SV
  TABLES (
    events   AS MAUDE_DB.CURATED.FACT_ADVERSE_EVENT PRIMARY KEY (mdr_report_key)
             COMMENT = 'One row per FDA medical device report (MDR)',
    devices  AS MAUDE_DB.CURATED.DIM_DEVICE
             COMMENT = 'Devices named on each report',
    patients AS MAUDE_DB.CURATED.PATIENT_OUTCOME
             COMMENT = 'Patient outcomes per report',
    problems AS MAUDE_DB.CURATED.BRIDGE_DEVICE_PROBLEM
             COMMENT = 'Product problem descriptions per report'
  )
  RELATIONSHIPS (
    dev_to_evt  AS devices  (mdr_report_key) REFERENCES events (mdr_report_key),
    pat_to_evt  AS patients (mdr_report_key) REFERENCES events (mdr_report_key),
    prob_to_evt AS problems (mdr_report_key) REFERENCES events (mdr_report_key)
  )
  DIMENSIONS (
    events.event_type        AS event_type        COMMENT = 'Death | Injury | Malfunction | Other',
    events.report_year       AS report_year       COMMENT = 'Year FDA received the report',
    events.report_source_code AS report_source_code,
    events.reporter_occupation AS reporter_occupation_code,
    events.manufacturer_name AS manufacturer_name,
    events.date_received     AS date_received,
    devices.brand_name       AS brand_name,
    devices.generic_name     AS generic_name,
    devices.product_code     AS product_code       COMMENT = 'FDA device product code',
    devices.device_class     AS device_class       COMMENT = 'FDA device class 1/2/3',
    devices.medical_specialty AS medical_specialty  COMMENT = 'FDA medical specialty panel',
    devices.device_manufacturer_name AS device_manufacturer_name,
    problems.device_problem  AS device_problem,
    patients.patient_sex     AS patient_sex
  )
  METRICS (
    events.report_count       AS COUNT(events.mdr_report_key) COMMENT = 'Number of MDRs',
    events.death_count        AS COUNT(CASE WHEN events.event_type = 'Death' THEN 1 END) COMMENT = 'MDRs coded as Death',
    events.injury_count       AS COUNT(CASE WHEN events.event_type = 'Injury' THEN 1 END) COMMENT = 'MDRs coded as Injury',
    events.malfunction_count  AS COUNT(CASE WHEN events.event_type = 'Malfunction' THEN 1 END) COMMENT = 'MDRs coded as Malfunction',
    events.avg_reporting_lag  AS AVG(events.reporting_lag_days) COMMENT = 'Avg days from event to FDA receipt'
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
      -- click-through link to the public FDA MAUDE detail record
      'https://www.accessdata.fda.gov/scripts/cdrh/cfdocs/cfMAUDE/detail.cfm?mdrfoi__id='
        || n.mdr_report_key AS source_url
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
