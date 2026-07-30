-- =====================================================================
-- MAUDE Ingestion - 03_curated.sql
-- Silver layer: typed star schema built as Dynamic Tables over RAW.
--
-- Grain:
--   FACT_ADVERSE_EVENT     1 row per MDR (mdr_report_key)
--   DIM_DEVICE             1 row per device on a report
--   EVENT_NARRATIVE        1 row per narrative segment (reporter / mfr)
--   PATIENT_OUTCOME        1 row per patient on a report
--   BRIDGE_DEVICE_PROBLEM  1 row per (report, product problem)
--
-- Refresh: TARGET_LAG '1 day', warehouse MAUDE_WH. FACT is a direct
-- projection (incremental-friendly). The FLATTEN-based children may
-- full-refresh; acceptable given the weekly source cadence. openFDA
-- dates are 'YYYYMMDD' strings -> TRY_TO_DATE.
--
-- Run as MAUDE_ENGINEER. Idempotent.
-- =====================================================================

USE ROLE MAUDE_ENGINEER;
USE WAREHOUSE MAUDE_WH;
USE DATABASE MAUDE_DB;
USE SCHEMA CURATED;

-- ---------------------------------------------------------------------
-- FACT_ADVERSE_EVENT - one row per MDR. Loader guarantees uniqueness of
-- mdr_report_key (per-partition delete+reload; keys unique across files).
-- ---------------------------------------------------------------------
CREATE OR REPLACE DYNAMIC TABLE FACT_ADVERSE_EVENT
  TARGET_LAG = '1 day'
  WAREHOUSE = MAUDE_WH
  REFRESH_MODE = AUTO
  INITIALIZE = ON_CREATE
  COMMENT = 'Master MDR event grain'
AS
SELECT
    report_key                                                    AS mdr_report_key,
    payload:event_type::string                                    AS event_type,
    payload:report_source_code::string                            AS report_source_code,
    ARRAY_TO_STRING(payload:source_type, '; ')                    AS source_type,
    payload:reporter_occupation_code::string                      AS reporter_occupation_code,
    payload:adverse_event_flag::string                            AS adverse_event_flag,
    payload:product_problem_flag::string                          AS product_problem_flag,
    ARRAY_TO_STRING(payload:type_of_report, '; ')                 AS type_of_report,
    payload:manufacturer_name::string                             AS manufacturer_name,
    payload:report_number::string                                 AS report_number,
    TRY_TO_NUMBER(payload:number_devices_in_event::string)        AS number_devices_in_event,
    TRY_TO_NUMBER(payload:number_patients_in_event::string)       AS number_patients_in_event,
    TRY_TO_DATE(payload:date_received::string,  'YYYYMMDD')       AS date_received,
    TRY_TO_DATE(payload:date_of_event::string,  'YYYYMMDD')       AS date_of_event,
    TRY_TO_DATE(payload:date_report::string,    'YYYYMMDD')       AS date_report,
    DATEDIFF('day',
        TRY_TO_DATE(payload:date_of_event::string, 'YYYYMMDD'),
        TRY_TO_DATE(payload:date_received::string, 'YYYYMMDD'))    AS reporting_lag_days,
    YEAR(TRY_TO_DATE(payload:date_received::string, 'YYYYMMDD'))   AS report_year,
    _partition,
    _export_date
FROM MAUDE_DB.RAW.RAW_DEVICE_EVENT;

-- ---------------------------------------------------------------------
-- DIM_DEVICE - device-level detail incl. openFDA classification.
-- ---------------------------------------------------------------------
CREATE OR REPLACE DYNAMIC TABLE DIM_DEVICE
  TARGET_LAG = '1 day'
  WAREHOUSE = MAUDE_WH
  REFRESH_MODE = AUTO
  INITIALIZE = ON_CREATE
  COMMENT = 'Device grain: brand/manufacturer/model + openFDA classification'
AS
SELECT
    r.report_key                                       AS mdr_report_key,
    dev.value:device_sequence_number::string           AS device_sequence_number,
    dev.value:brand_name::string                       AS brand_name,
    dev.value:generic_name::string                     AS generic_name,
    dev.value:manufacturer_d_name::string              AS device_manufacturer_name,
    dev.value:model_number::string                     AS model_number,
    dev.value:catalog_number::string                   AS catalog_number,
    dev.value:lot_number::string                       AS lot_number,
    dev.value:device_report_product_code::string       AS product_code,
    dev.value:implant_flag::string                     AS implant_flag,
    dev.value:udi_di::string                           AS udi_di,
    dev.value:openfda:device_class::string             AS device_class,
    dev.value:openfda:device_name::string              AS openfda_device_name,
    dev.value:openfda:medical_specialty_description::string AS medical_specialty,
    dev.value:openfda:regulation_number::string        AS regulation_number,
    r._partition
FROM MAUDE_DB.RAW.RAW_DEVICE_EVENT r,
     LATERAL FLATTEN(input => r.payload:device) dev;

-- ---------------------------------------------------------------------
-- EVENT_NARRATIVE - the free-text clinical/manufacturer narratives.
-- redaction_flag marks FOIA-redacted text ((b)(4) / (b)(6)).
-- This is the corpus for Cortex Search + AI enrichment.
-- ---------------------------------------------------------------------
CREATE OR REPLACE DYNAMIC TABLE EVENT_NARRATIVE
  TARGET_LAG = '1 day'
  WAREHOUSE = MAUDE_WH
  REFRESH_MODE = AUTO
  INITIALIZE = ON_CREATE
  COMMENT = 'MDR narrative text (reporter + manufacturer), one row per segment'
AS
SELECT
    r.report_key                                       AS mdr_report_key,
    mt.value:text_type_code::string                    AS text_type_code,
    mt.value:text::string                              AS narrative_text,
    LENGTH(mt.value:text::string)                      AS narrative_length,
    (mt.value:text::string ILIKE '%(b)(4)%'
     OR mt.value:text::string ILIKE '%(b)(6)%')        AS redaction_flag,
    r._partition
FROM MAUDE_DB.RAW.RAW_DEVICE_EVENT r,
     LATERAL FLATTEN(input => r.payload:mdr_text) mt
WHERE mt.value:text IS NOT NULL;

-- ---------------------------------------------------------------------
-- PATIENT_OUTCOME - patient-level detail (public, de-identified;
-- age/etc. may be (b)(6)-redacted at source).
-- ---------------------------------------------------------------------
CREATE OR REPLACE DYNAMIC TABLE PATIENT_OUTCOME
  TARGET_LAG = '1 day'
  WAREHOUSE = MAUDE_WH
  REFRESH_MODE = AUTO
  INITIALIZE = ON_CREATE
  COMMENT = 'Patient grain: sex/age/weight and coded outcomes'
AS
SELECT
    r.report_key                                       AS mdr_report_key,
    pt.value:patient_sequence_number::string           AS patient_sequence_number,
    pt.value:patient_age::string                       AS patient_age,
    pt.value:patient_sex::string                       AS patient_sex,
    pt.value:patient_weight::string                    AS patient_weight,
    -- TO_JSON is robust to occasional nested/non-scalar array elements
    -- that would break ARRAY_TO_STRING on this passive-surveillance data.
    TO_JSON(pt.value:patient_problems)                 AS patient_problems,
    TO_JSON(pt.value:sequence_number_outcome)          AS outcome_codes,
    r._partition
FROM MAUDE_DB.RAW.RAW_DEVICE_EVENT r,
     LATERAL FLATTEN(input => r.payload:patient) pt;

-- ---------------------------------------------------------------------
-- BRIDGE_DEVICE_PROBLEM - product problem codes from the master record.
-- ---------------------------------------------------------------------
CREATE OR REPLACE DYNAMIC TABLE BRIDGE_DEVICE_PROBLEM
  TARGET_LAG = '1 day'
  WAREHOUSE = MAUDE_WH
  REFRESH_MODE = AUTO
  INITIALIZE = ON_CREATE
  COMMENT = 'One row per (MDR, product problem description)'
AS
SELECT
    r.report_key           AS mdr_report_key,
    pp.value::string        AS device_problem,
    r._partition
FROM MAUDE_DB.RAW.RAW_DEVICE_EVENT r,
     LATERAL FLATTEN(input => r.payload:product_problems) pp
WHERE pp.value IS NOT NULL;
