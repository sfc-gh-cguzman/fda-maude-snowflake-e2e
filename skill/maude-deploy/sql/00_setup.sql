-- =====================================================================
-- MAUDE Ingestion - 00_setup.sql
-- Provision database, schemas, warehouse, roles, and External Access
-- Integration for the openFDA MAUDE (device/event) pipeline.
--
-- Run as a role that can assume ACCOUNTADMIN (EAI + network rule creation).
-- Idempotent: safe to re-run.
-- =====================================================================

-- ---------------------------------------------------------------------
-- Warehouse
-- ---------------------------------------------------------------------
USE ROLE SYSADMIN;

CREATE WAREHOUSE IF NOT EXISTS MAUDE_WH
  WAREHOUSE_SIZE = 'MEDIUM'          -- sized for the ~17.6 GB historical backfill
  AUTO_SUSPEND = 60
  AUTO_RESUME = TRUE
  INITIALLY_SUSPENDED = TRUE
  COMMENT = 'MAUDE ingestion + analytics warehouse';

-- ---------------------------------------------------------------------
-- Database + medallion schemas
-- ---------------------------------------------------------------------
CREATE DATABASE IF NOT EXISTS MAUDE_DB
  COMMENT = 'FDA MAUDE device adverse event lakehouse';

CREATE SCHEMA IF NOT EXISTS MAUDE_DB.RAW       COMMENT = 'Bronze: staged openFDA JSON landed as VARIANT';
CREATE SCHEMA IF NOT EXISTS MAUDE_DB.CURATED   COMMENT = 'Silver: typed star schema (Dynamic Tables)';
CREATE SCHEMA IF NOT EXISTS MAUDE_DB.ANALYTICS COMMENT = 'Gold: semantic view, Cortex Search, AI enrichment, agents';

-- ---------------------------------------------------------------------
-- Roles
--   MAUDE_ENGINEER  - builds and loads the pipeline
--   MAUDE_CLINICIAN - read-only ANALYTICS + agent usage
-- ---------------------------------------------------------------------
USE ROLE SECURITYADMIN;

CREATE ROLE IF NOT EXISTS MAUDE_ENGINEER;
CREATE ROLE IF NOT EXISTS MAUDE_CLINICIAN;

GRANT ROLE MAUDE_ENGINEER  TO ROLE SYSADMIN;
GRANT ROLE MAUDE_CLINICIAN TO ROLE SYSADMIN;

-- Warehouse usage
GRANT USAGE ON WAREHOUSE MAUDE_WH TO ROLE MAUDE_ENGINEER;
GRANT USAGE ON WAREHOUSE MAUDE_WH TO ROLE MAUDE_CLINICIAN;

-- Engineer: full control of the database
GRANT USAGE ON DATABASE MAUDE_DB TO ROLE MAUDE_ENGINEER;
GRANT USAGE ON ALL SCHEMAS IN DATABASE MAUDE_DB TO ROLE MAUDE_ENGINEER;
GRANT ALL PRIVILEGES ON SCHEMA MAUDE_DB.RAW       TO ROLE MAUDE_ENGINEER;
GRANT ALL PRIVILEGES ON SCHEMA MAUDE_DB.CURATED   TO ROLE MAUDE_ENGINEER;
GRANT ALL PRIVILEGES ON SCHEMA MAUDE_DB.ANALYTICS TO ROLE MAUDE_ENGINEER;

-- Clinician: read-only on ANALYTICS only (no RAW / CURATED access)
GRANT USAGE ON DATABASE MAUDE_DB TO ROLE MAUDE_CLINICIAN;
GRANT USAGE ON SCHEMA MAUDE_DB.ANALYTICS TO ROLE MAUDE_CLINICIAN;
GRANT SELECT ON ALL TABLES    IN SCHEMA MAUDE_DB.ANALYTICS TO ROLE MAUDE_CLINICIAN;
GRANT SELECT ON FUTURE TABLES IN SCHEMA MAUDE_DB.ANALYTICS TO ROLE MAUDE_CLINICIAN;
GRANT SELECT ON ALL VIEWS     IN SCHEMA MAUDE_DB.ANALYTICS TO ROLE MAUDE_CLINICIAN;
GRANT SELECT ON FUTURE VIEWS  IN SCHEMA MAUDE_DB.ANALYTICS TO ROLE MAUDE_CLINICIAN;

-- Let the engineer role own future objects created here
USE ROLE SYSADMIN;
GRANT OWNERSHIP ON SCHEMA MAUDE_DB.RAW       TO ROLE MAUDE_ENGINEER REVOKE CURRENT GRANTS;
GRANT OWNERSHIP ON SCHEMA MAUDE_DB.CURATED   TO ROLE MAUDE_ENGINEER REVOKE CURRENT GRANTS;
GRANT OWNERSHIP ON SCHEMA MAUDE_DB.ANALYTICS TO ROLE MAUDE_ENGINEER REVOKE CURRENT GRANTS;
GRANT OWNERSHIP ON DATABASE MAUDE_DB TO ROLE MAUDE_ENGINEER REVOKE CURRENT GRANTS;

-- ---------------------------------------------------------------------
-- External Access Integration to openFDA
--   download.open.fda.gov  - bulk JSON partition zips
--   api.fda.gov            - download.json manifest
-- No secret / API key required (public, CC0). API key optional for higher
-- rate limits but not needed for the bulk-download path.
-- ---------------------------------------------------------------------
USE ROLE ACCOUNTADMIN;

CREATE OR REPLACE NETWORK RULE MAUDE_DB.RAW.OPENFDA_NETWORK_RULE
  MODE = EGRESS
  TYPE = HOST_PORT
  VALUE_LIST = ('download.open.fda.gov', 'api.fda.gov')
  COMMENT = 'Egress to openFDA download + manifest hosts';

CREATE OR REPLACE EXTERNAL ACCESS INTEGRATION MAUDE_OPENFDA_EAI
  ALLOWED_NETWORK_RULES = (MAUDE_DB.RAW.OPENFDA_NETWORK_RULE)
  ENABLED = TRUE
  COMMENT = 'External access for the MAUDE openFDA ingestion procs';

GRANT USAGE ON INTEGRATION MAUDE_OPENFDA_EAI TO ROLE MAUDE_ENGINEER;

-- Allow the engineer role to run serverless / managed tasks (backfill + sync)
GRANT EXECUTE MANAGED TASK ON ACCOUNT TO ROLE MAUDE_ENGINEER;
GRANT EXECUTE TASK         ON ACCOUNT TO ROLE MAUDE_ENGINEER;

-- ---------------------------------------------------------------------
-- Context for the rest of the build
-- ---------------------------------------------------------------------
USE ROLE MAUDE_ENGINEER;
USE WAREHOUSE MAUDE_WH;
USE DATABASE MAUDE_DB;
USE SCHEMA RAW;
