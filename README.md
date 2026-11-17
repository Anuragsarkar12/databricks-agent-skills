# Databricks Agent Skills

A curated repository of Databricks-focused AI coding skills and evaluation assets designed to help agents generate safer, more production-ready code for common lakehouse workflows.

This project packages operational knowledge as reusable `SKILL.md` playbooks for high-value tasks such as Databricks Asset Bundles, data quality enforcement, Delta table patterns, incremental ingestion, and performance tuning. The goal is to reduce boilerplate, standardize best practices, and make AI-assisted engineering more reliable in Databricks environments.

## Why this repository exists

Databricks teams repeatedly build the same foundational patterns:

- Incremental ingestion with Auto Loader
- CDC and SCD handling with Delta `MERGE INTO`
- Data quality expectations in Lakeflow
- Deployment via Databricks Asset Bundles
- Table optimization and cluster tuning
- Spark testing in CI/CD workflows

The challenge is not simply writing the code once; it is writing it consistently, safely, and in a way that matches production expectations. This repository captures those patterns as reusable skill definitions so an AI agent can follow the same guardrails each time.

## Repository layout

```text
.
├── README.md
├── databricks_skill_strategy.md
├── final task list.md
├── pillars_triggerability_reliability_safety.md
├── skill_evaluation_architecture.md
├── skills_best_practices.md
├── v2_roadmap.md
├── evals/
│   ├── configuring-databricks-asset-bundles.json
│   └── evals (1).json
├── skills/
│   ├── 01_databricks_asset_bundles_cicd_configuration.md
│   ├── 02_data_quality_sdp_expectations_lakehouse_monitoring.md
│   ├── 03_delta_scd_type_1_and_2_merge_into.md
│   ├── 04_auto_loader_incremental_ingestion.md
│   ├── 05_delta_schema_evolution_management.md
│   ├── 06_optimizing_skewed_pyspark_joins_aggregations.md
│   └── importing-bi-dashboards/
│       └── SKILL.md
└── ...
```

## What is included

### Core strategy and planning documents

These files capture the rationale, roadmap, and evaluation methods behind the skill library:

- `databricks_skill_strategy.md` — project strategy and framing
- `final task list.md` — prioritized tasks and focus areas
- `pillars_triggerability_reliability_safety.md` — strategy pillars
- `skill_evaluation_architecture.md` — architecture for evaluating skill quality
- `skills_best_practices.md` — best practices for authoring and using skills
- `v2_roadmap.md` — future roadmap and evolution plan

### Skill definitions

The `skills/` folder contains reusable Databricks skill descriptions. These are written as AI-consumable instructions that encode domain-specific rules, canonical examples, and safety expectations.

Current skill areas include:

1. Databricks Asset Bundles CI/CD configuration
2. Data quality with SDP expectations and Lakehouse Monitoring
3. Delta SCD Type 1 and Type 2 using `MERGE INTO`
4. Auto Loader incremental ingestion
5. Delta schema evolution management
6. Optimizing skewed PySpark joins and aggregations
7. Importing BI dashboards

### Evaluation assets

The `evals/` folder contains evaluation scenarios used to test skill quality and reliability. These are useful when validating whether a skill produces correct, consistent, and production-aware outputs.

## Typical usage

This repository is meant to support AI-assisted engineering workflows, especially in environments where an LLM agent is being used to generate or modify Databricks assets.

A typical flow looks like this:

1. Select a relevant skill from the `skills/` directory.
2. Provide the agent with a prompt that reflects the engineering task.
3. Let the agent apply the skill's conventions, templates, and guardrails.
4. Review the generated YAML, SQL, or PySpark against your environment.
5. Validate the output with tests, bundles, or pipeline execution.

## Recommended operating model

### For teams

Use these skills as a shared standard for:

- Data platform governance
- Common ETL and transformation patterns
- CI/CD deployment workflows
- Data quality enforcement
- Avoiding repeated boilerplate and drift

### For AI agents

The skill files serve as a compact domain guide, helping the agent:

- choose the correct Databricks API or pattern
- follow best practices instead of guessing
- prefer safe, production-aware defaults
- generate outputs that match the broader data engineering standard used by the organization

## Example skill topics in this project

### Databricks Asset Bundles

Covers multi-environment deployment definitions, variable substitution, service principal execution, job/resource scaffolding, and bundle structure conventions.

### Data quality

Covers `@dlt.expect`, `@dlt.expect_or_drop`, `@dlt.expect_or_fail`, and Lakehouse Monitoring for validating data before it reaches downstream layers.

### Delta patterns

Covers SCD Type 1 and Type 2 merge logic, idempotency concerns, `valid_from` / `valid_to` approaches, and merge-key design.

### Auto Loader

Explains streaming file ingestion, checkpointing, schema evolution, and triggering patterns for incremental ingestion.

### Performance tuning

Covers skew mitigation strategies for distributed joins and aggregations so that generated PySpark code is not only functional but operationally viable.

## Project status

This repository is a documentation and skill library focused on Databricks data engineering best practices. It is not a packaged application or service and does not currently include a runtime deployment target or production codebase.

## Contribution guidance

If you expand this repository, keep the following in mind:

- Prefer production-safe defaults over simplified examples.
- Document exact Databricks patterns and constraints.
- Keep skills narrow, specific, and operationally useful.
- Include examples and guardrails, not just abstract guidance.
- Favor reproducible quality checks and explicit failure modes.

## Notes

This repository is designed to support agent-driven Databricks work, especially when the goal is to standardize common lakehouse tasks and reduce the chance of fragile, one-off implementations.

