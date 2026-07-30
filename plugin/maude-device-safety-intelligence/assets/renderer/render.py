#!/usr/bin/env python3
"""Deterministic renderer for the MAUDE Device Safety Intelligence deployment.

Reads build_manifest.yaml plus the Jinja SQL templates and writes one .sql file
per target object, substituting the deployment-specific names (target database,
warehouse, roles) from a config JSON.

No database access -- this is a pure text transform. The maude-deploy skill
renders first, then executes the emitted SQL in build_manifest order.

Unlike accelerators that map an unknown customer schema, MAUDE's source is fixed
(openFDA device/event is identical for every deployment), so there is no
conformance contract to resolve. The only things that vary per deployment are the
container names, which is all this renderer injects.

Template helpers exposed as Jinja globals:
  db()                     -> target database name
  target(schema, object)   -> TARGET_DATABASE.SCHEMA.OBJECT
  warehouse                -> compute warehouse name
  engineer_role            -> pipeline owner role
  clinician_role           -> read-only consumer role
  eai_name                 -> external access integration name
  target_lag               -> Dynamic Table / Cortex Search target lag
  load_scope               -> full | last_10_years | last_5_years
  enrichment_sample_rows   -> bounded AI_CLASSIFY sample size (cost guard)
  enrichment_min_corpus_rows -> guard threshold before enrichment may run
  cfg                      -> the raw config dict
"""
import argparse
import json
import os
from pathlib import Path

import yaml
from jinja2 import Environment, FileSystemLoader, StrictUndefined


DEFAULTS = {
    "target_database": "MAUDE_DB",
    "warehouse": "MAUDE_WH",
    "engineer_role": "MAUDE_ENGINEER",
    "clinician_role": "MAUDE_CLINICIAN",
    "eai_name": "MAUDE_OPENFDA_EAI",
    "target_lag": "1 day",
    "load_scope": "full",
    # AI_CLASSIFY is billed per token; keep the shipped sample bounded.
    "enrichment_sample_rows": 200,
    # Refuse to enrich until the backfill has actually landed narratives.
    "enrichment_min_corpus_rows": 100000,
}

VALID_SCOPES = {"full", "last_10_years", "last_5_years"}


def build_env(templates_dir, config):
    def db():
        return config["target_database"]

    def target(schema, obj):
        return f"{config['target_database']}.{schema}.{obj}"

    env = Environment(
        loader=FileSystemLoader(str(templates_dir)),
        undefined=StrictUndefined,
        trim_blocks=False,
        lstrip_blocks=False,
        keep_trailing_newline=True,
    )
    env.globals.update(
        db=db,
        target=target,
        cfg=config,
        warehouse=config["warehouse"],
        engineer_role=config["engineer_role"],
        clinician_role=config["clinician_role"],
        eai_name=config["eai_name"],
        target_lag=config["target_lag"],
        load_scope=config["load_scope"],
        enrichment_sample_rows=config["enrichment_sample_rows"],
        enrichment_min_corpus_rows=config["enrichment_min_corpus_rows"],
    )
    return env


def load_config(path):
    cfg = dict(DEFAULTS)
    if path:
        cfg.update(json.load(open(path)))
    if cfg["load_scope"] not in VALID_SCOPES:
        raise ValueError(
            f"load_scope '{cfg['load_scope']}' invalid; expected one of {sorted(VALID_SCOPES)}"
        )
    for k in ("enrichment_sample_rows", "enrichment_min_corpus_rows"):
        if not isinstance(cfg[k], int) or cfg[k] < 1:
            raise ValueError(f"{k} must be a positive integer, got {cfg[k]!r}")
    return cfg


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--manifest", required=True)
    ap.add_argument("--templates", required=True)
    ap.add_argument("--out", required=True)
    ap.add_argument("--config", help="JSON overriding target_database/warehouse/roles/scope")
    ap.add_argument("--only", nargs="*", help="render only these object_names")
    a = ap.parse_args()

    config = load_config(a.config)
    env = build_env(a.templates, config)
    manifest = yaml.safe_load(open(a.manifest))

    os.makedirs(a.out, exist_ok=True)
    rendered = []
    for obj in manifest["objects"]:
        name = obj["object_name"]
        if a.only and name not in a.only:
            continue
        tmpl = env.get_template(obj["template"])
        sql = tmpl.render(obj=obj, params=obj.get("params", {}))
        outp = Path(a.out) / f"{name}.sql"
        outp.write_text(sql)
        rendered.append(str(outp))
    print(json.dumps({"config": config, "rendered": rendered}, indent=2))


if __name__ == "__main__":
    main()
