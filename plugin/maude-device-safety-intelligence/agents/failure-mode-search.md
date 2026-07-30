# MAUDE Failure-Mode Search Agent

Cortex Agent for narrative RAG over FDA MAUDE adverse-event reports. Surfaces
specific device failure modes and clinical presentations with cited MDR evidence.

## Tools

| Tool | Type | Purpose |
|---|---|---|
| narrative_search | cortex_search | Semantic search over ~58M narrative segments (MAUDE_NARRATIVE_SEARCH) |

## FQN

`<TARGET_DB>.ANALYTICS.MAUDE_FAILURE_MODE_AGENT`

## Persona

Quality/CAPA engineers, R&D design engineers, field safety officers, and
clinical/medical affairs professionals looking for specific failure evidence.

## Sample questions

- Find reports describing catheter tip fracture during retrieval.
- What failure modes are described for infusion pump software errors?
- Show reports mentioning lead insulation failure in pacemakers.
- Find narratives describing device migration after implantation.
- What do reports say about balloon rupture during angioplasty?

## Citations

Same citation model as the Device Safety Profile agent:
`mdr_text_key` (the unique citation id), `mdr_report_key`, `report_number`,
`citation_title`, `source_url` (openFDA API record for verification).

## Governance

Response template always ends with the FDA passive-surveillance disclaimer.
Redacted narratives (FOIA `(b)(4)`/`(b)(6)`) are flagged via `redaction_flag`.
