# MAUDE Device Safety Profile Agent

Cortex Agent for structured + unstructured device-safety intelligence over the
FDA MAUDE database (~25.4M MDRs, ~58M narrative segments).

## Tools

| Tool | Type | Purpose |
|---|---|---|
| safety_analyst | cortex_analyst_text_to_sql | Counts, trends, breakdowns via MAUDE_SAFETY_SV semantic view |
| narrative_search | cortex_search | Semantic RAG over adverse-event narratives (MAUDE_NARRATIVE_SEARCH) |
| data_to_chart | data_to_chart | Visualize trends and comparisons |

## FQN

`<TARGET_DB>.ANALYTICS.MAUDE_DEVICE_SAFETY_AGENT`

## Persona

Clinicians, safety officers, quality/CAPA engineers, RA professionals, and
procurement/value analysis committees at medical device companies.

## Sample questions (hybrid: structured + unstructured)

- How many malfunction reports were filed for insulin pumps last year, and what failure modes do the narratives most often describe?
- Which product code has the most death reports, and summarize what those narratives say happened?
- Compare injury vs malfunction counts for coronary stents by year, then pull example narratives from the year with the biggest spike.

## Citations

Every response cites:
- `mdr_report_key` — FDA canonical identifier
- `report_number` — human-readable MDR number
- `citation_title` — "brand_name - event_type, year"
- `source_url` — clickable deep link to the FDA MAUDE detail page

## Governance

Response template always ends with:
> MAUDE is a passive surveillance system. Report counts cannot establish event
> rates, incidence, or causation, and this information must not be used for
> individual patient-care decisions.
