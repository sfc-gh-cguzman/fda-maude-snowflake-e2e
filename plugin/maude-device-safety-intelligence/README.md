# MAUDE Device Safety Intelligence

A CoCo plugin that deploys the FDA MAUDE device adverse-event pipeline + clinician Cortex Agents in any Snowflake account.

## What it does

Ingests the full openFDA `device/event` dataset (~25.4M medical device reports, ~58M narrative segments), curates it into an analytics-ready star schema, and exposes it through Cortex Agents for device-safety intelligence.

- **Source:** openFDA bulk JSON (CC0/public domain, no license/agreement needed)
- **Refresh:** weekly incremental sync (serverless, seconds of compute)
- **Serving:** semantic view (Cortex Analyst), Cortex Search over narratives, AI enrichment (AI_CLASSIFY), two Cortex Agents with FDA source-URL citations

## Skills

| Skill | Purpose |
|---|---|
| `/maude-deploy` | Guided workflow to deploy the full pipeline in a target account (scope choice: full/10yr/5yr history) |

## Agents deployed

| Agent | Tools | Purpose |
|---|---|---|
| MAUDE_DEVICE_SAFETY_AGENT | Analyst + Search + Charts | Structured counts/trends + cited narrative evidence |
| MAUDE_FAILURE_MODE_AGENT | Search | Narrative RAG for specific failure modes |

## Personas

- Postmarket surveillance / complaint handling
- Quality / CAPA engineers
- R&D / design engineers (ISO 14971 risk inputs)
- Regulatory Affairs (510(k)/PMA, CER evidence)
- Clinical / Medical Affairs (HCP inquiry response)
- Clinical / field safety officers
- Procurement / value analysis committees

## Governance

- All agent responses carry the FDA passive-surveillance disclaimer
- Citations include clickable deep links to official FDA MAUDE records
- RBAC: `MAUDE_CLINICIAN` role for read-only ANALYTICS + agent usage
- FOIA redactions (`(b)(4)` trade secret, `(b)(6)` patient) flagged, not hidden

## Credits and cost

- Backfill (one-time): ~45 min full / ~22 min 5-year, parallel XSMALL lanes
- Weekly sync: XS serverless, seconds per run (negligible)
- Source data: free (openFDA CC0)

## Install

```
/maude-deploy
```

Or install the plugin from the catalog, then run the skill.
