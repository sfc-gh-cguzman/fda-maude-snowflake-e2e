-- =====================================================================
-- MAUDE Ingestion - 05_agents.sql
-- Clinician-facing Cortex Agents for device safety intelligence.
--
-- Framing: POSTMARKET SURVEILLANCE, not individual patient care. MAUDE
-- cannot establish event rates or causation. Every agent response must
-- carry that disclaimer and cite MDR report keys.
--
-- Agent 1: MAUDE_DEVICE_SAFETY_AGENT
--   Cortex Analyst (semantic view) + Cortex Search (narratives).
--   "What has been reported for device/brand/product code X?"
-- Agent 2: MAUDE_FAILURE_MODE_AGENT
--   Cortex Search-led RAG over 25M narratives for specific failure modes.
--
-- Run as MAUDE_ENGINEER. Idempotent.
-- =====================================================================

USE ROLE MAUDE_ENGINEER;
USE WAREHOUSE MAUDE_WH;
USE DATABASE MAUDE_DB;
USE SCHEMA ANALYTICS;

-- ---------------------------------------------------------------------
-- Agent 1: Device Safety Profile
-- ---------------------------------------------------------------------
CREATE OR REPLACE AGENT MAUDE_DEVICE_SAFETY_AGENT
WITH PROFILE = '{"display_name": "MAUDE Device Safety Profile"}'
COMMENT = 'Device safety intelligence over FDA MAUDE (structured + narrative). Postmarket surveillance only.'
FROM SPECIFICATION $$
{
  "models": { "orchestration": "auto" },
  "instructions": {
    "system": "You are a medical device postmarket safety analyst for clinicians. You answer questions about adverse events reported to the FDA MAUDE database. You have two tools: safety_analyst for counts, trends, and breakdowns; narrative_search for retrieving specific report narratives. Always use safety_analyst for any quantitative question. Always cite the mdr_report_key for narrative evidence.",
    "response": "Be concise and clinical. Lead with the numbers, then supporting narrative examples. For each cited report, include: mdr_report_key, report_number, and the FDA source_url link so the user can verify the original MDR directly. ALWAYS end with this disclaimer: 'MAUDE is a passive surveillance system. Report counts cannot establish event rates, incidence, or causation, and this information must not be used for individual patient-care decisions.'",
    "orchestration": "Use safety_analyst for any question involving how many, trends, comparisons, or breakdowns by device, manufacturer, product code, event type, or year. Use narrative_search when the clinician asks what happened, for examples, or to describe a failure mode.",
    "sample_questions": [
      { "question": "How many malfunction reports were filed for infusion pumps in the last 3 years?" },
      { "question": "Compare death, injury, and malfunction report counts for coronary stents by year." },
      { "question": "Which manufacturers have the most adverse-event reports for surgical staplers?" },
      { "question": "What are the most common product problems reported for insulin pumps, and what patient outcomes were noted?" },
      { "question": "Show the trend of reports for a given product code over time." }
    ]
  },
  "tools": [
    { "tool_spec": { "type": "cortex_analyst_text_to_sql", "name": "safety_analyst" } },
    { "tool_spec": { "type": "cortex_search", "name": "narrative_search" } },
    { "tool_spec": { "type": "data_to_chart", "name": "data_to_chart", "description": "Generates charts from query results (trends, comparisons)" } }
  ],
  "tool_resources": {
    "safety_analyst": {
      "semantic_view": "MAUDE_DB.ANALYTICS.MAUDE_SAFETY_SV",
      "execution_environment": { "type": "warehouse", "warehouse": "MAUDE_WH", "query_timeout": 299 }
    },
    "narrative_search": {
      "name": "MAUDE_DB.ANALYTICS.MAUDE_NARRATIVE_SEARCH",
      "id_column": "mdr_report_key",
      "title_column": "citation_title",
      "max_results": 10,
      "execution_environment": { "type": "warehouse", "warehouse": "MAUDE_WH", "query_timeout": 299 }
    }
  }
}
$$;

-- ---------------------------------------------------------------------
-- Agent 2: Failure-Mode Narrative Search
-- ---------------------------------------------------------------------
CREATE OR REPLACE AGENT MAUDE_FAILURE_MODE_AGENT
WITH PROFILE = '{"display_name": "MAUDE Failure-Mode Search"}'
COMMENT = 'RAG over FDA MAUDE narratives to surface specific device failure modes with citations.'
FROM SPECIFICATION $$
{
  "models": { "orchestration": "auto" },
  "instructions": {
    "system": "You help clinicians and safety officers find FDA MAUDE adverse-event reports describing specific device failure modes or clinical presentations. Retrieve the most relevant report narratives and summarize the common failure patterns.",
    "response": "Summarize the failure patterns you find, then list the supporting reports with their mdr_report_key, report_number, event_type, and source_url (FDA deep link). If a narrative is marked redacted, note that trade-secret/patient text was removed by FDA. ALWAYS end with: 'MAUDE is a passive surveillance system. Report counts cannot establish event rates, incidence, or causation, and this information must not be used for individual patient-care decisions.'",
    "orchestration": "Always call narrative_search first. Use the returned attributes (product_code, brand_name, event_type) to group and explain the findings.",
    "sample_questions": [
      { "question": "Find reports describing catheter tip fracture during retrieval." },
      { "question": "What failure modes are described for infusion pump software errors?" },
      { "question": "Show reports mentioning lead insulation failure in pacemakers." },
      { "question": "Find narratives describing device migration after implantation." },
      { "question": "What do reports say about balloon rupture during angioplasty?" }
    ]
  },
  "tools": [
    { "tool_spec": { "type": "cortex_search", "name": "narrative_search" } }
  ],
  "tool_resources": {
    "narrative_search": {
      "name": "MAUDE_DB.ANALYTICS.MAUDE_NARRATIVE_SEARCH",
      "id_column": "mdr_report_key",
      "title_column": "citation_title",
      "max_results": 15,
      "execution_environment": { "type": "warehouse", "warehouse": "MAUDE_WH", "query_timeout": 299 }
    }
  }
}
$$;

-- ---------------------------------------------------------------------
-- Clinician usage grants
-- ---------------------------------------------------------------------
GRANT USAGE ON AGENT MAUDE_DB.ANALYTICS.MAUDE_DEVICE_SAFETY_AGENT TO ROLE MAUDE_CLINICIAN;
GRANT USAGE ON AGENT MAUDE_DB.ANALYTICS.MAUDE_FAILURE_MODE_AGENT  TO ROLE MAUDE_CLINICIAN;
