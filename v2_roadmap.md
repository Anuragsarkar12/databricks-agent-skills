# SKILL.md Library — v2 Roadmap

> This document outlines the planned enhancements for the next iteration of the data engineering agent skill library. It is informed by the critical analysis conducted during v1 generation.

---

## 1. Error Handling Patterns

**Problem:** None of the v1 skills address runtime failures. Generated code has no try/except blocks, no dead-letter routing, and no idempotency guarantees.

**Planned additions:**

### Per-Skill Error Handling

| Skill | Error Handling Enhancement |
|-------|--------------------------|
| 01 — DAB CI/CD | Add `on_failure` task configuration in job definitions (email/webhook notification on task failure) |
| 02 — Data Quality | Add quarantine table pattern: route `expect_or_drop` dropped rows to a `_quarantine` table for investigation instead of silent discard |
| 03 — Delta SCD | Add idempotency guard for SCD Type 2 two-phase pattern: check for already-expired rows before running Phase 1 to prevent inconsistent state on retry |
| 04 — Auto Loader | Add `cloudFiles.badRecordsPath` configuration for routing unparseable files to a dead-letter location; add stream error recovery pattern with `foreachBatch` |
| 05 — Schema Evolution | Add `RESTORE TABLE ... TO VERSION AS OF` rollback automation if ALTER TABLE fails mid-execution |
| 06 — Skewed Joins | Add diagnostic fallback: if profiling query itself OOMs (table too large for count()), provide a sampled profiling pattern using `.sample()` |

### Cross-Cutting Error Template

Every skill should include a standard error wrapper:

```python
try:
    # Generated pipeline code
    ...
except AnalysisException as e:
    if "cannot resolve" in str(e):
        # Schema mismatch — log and alert
        ...
    raise
except Py4JJavaError as e:
    if "OutOfMemoryError" in str(e):
        # OOM — reduce batch size and retry
        ...
    raise
```

---

## 2. Composability / Orchestration Skill

**Problem:** The 6 skills define boundaries but never describe how to chain them into an end-to-end pipeline. An agent cannot currently compose: Auto Loader (04) → Data Quality (02) → SCD MERGE (03) → Schema Evolution (05) → Bundle Config (01).

**Planned skill:** `07_pipeline_orchestration_and_composition.md`

### Scope

- Define canonical pipeline stages: Ingest → Validate → Transform → Load → Monitor
- Map each stage to the responsible skill
- Provide inter-skill handoff patterns (e.g., output of Auto Loader becomes input to DLT expectations)
- Define task dependency graphs in `databricks.yml` that chain tasks across skill boundaries
- Address cross-skill concerns: shared variable naming, consistent checkpoint path conventions, unified PII masking across all stages

### Architecture

```
┌─────────────┐    ┌─────────────┐    ┌─────────────┐    ┌─────────────┐
│  Skill 04   │───▶│  Skill 02   │───▶│  Skill 03   │───▶│  Skill 02   │
│ Auto Loader │    │ DQ Expects  │    │  SCD MERGE  │    │  Monitoring │
│  (Bronze)   │    │  (Silver)   │    │   (Gold)    │    │  (Observe)  │
└─────────────┘    └─────────────┘    └─────────────┘    └─────────────┘
        │                                                        │
        └──────────── Skill 01: DAB Bundle Config ───────────────┘
                      Skill 05: Schema Evolution (as needed)
                      Skill 06: Skew Optimisation (as needed)
```

---

## 3. Cost-Awareness Annotations

**Problem:** v1 skills recommend features (Photon, Liquid Clustering, broadcast joins, key salting) without any cost context. Enterprise teams need to make trade-offs between performance and compute cost.

**Planned additions per skill:**

| Skill | Cost Annotation |
|-------|----------------|
| 01 — DAB CI/CD | Add `photon: true` cost note: "Photon acceleration increases DBU cost by approximately 2x. Set `photon: false` for dev targets to reduce development costs." |
| 02 — Data Quality | Add Lakehouse Monitoring cost note: "CREATE MONITOR runs profiling queries on a schedule. Each run consumes SQL Warehouse DBUs. Set SCHEDULE CRON frequency based on monitoring SLA, not maximum frequency." |
| 03 — Delta SCD | Add Liquid Clustering cost note: "CLUSTER BY triggers background data rewriting. On tables with frequent MERGE operations, clustering overhead compounds. Monitor OPTIMIZE history to assess cumulative cost." |
| 04 — Auto Loader | Add notification mode cost note: "File notification mode creates cloud infrastructure (SQS queues, Event Grid subscriptions, Pub/Sub topics) that incur cloud provider charges independent of Databricks billing." |
| 05 — Schema Evolution | Add protocol upgrade cost note: "Upgrading minReaderVersion/minWriterVersion is irreversible. Older runtimes cannot read the table, which may force cluster upgrades across all consuming teams." |
| 06 — Skewed Joins | Add salting cost note: "Key salting replicates the smaller table by SALT_BUCKETS factor, increasing shuffle data volume and memory consumption. Verify that the cost of salting does not exceed the cost of the straggler." |

### Format

Cost annotations should appear as a standardised callout after each recommendation:

```markdown
> **Cost impact:** <description of cost trade-off and how to evaluate>
```

---

## 4. Expanded Resource Type Coverage (Skill 01)

**Problem:** Skill 01 only covers `jobs` and `pipelines` resource types. DAB supports additional resource types needed for ML Ops and data governance workflows.

**Planned additions:**

| Resource Type | Use Case |
|--------------|----------|
| `model_serving_endpoints` | Deploy and manage ML model serving endpoints via bundle config |
| `registered_models` | Register ML models in Unity Catalog via bundle |
| `experiments` | Configure MLflow experiment tracking paths |
| `schemas` | Create and manage Unity Catalog schemas as bundle resources |
| `quality_monitors` | Deploy Lakehouse Monitoring configurations as code |
| `clusters` | Define interactive cluster configurations (for teams that manage clusters via bundles) |

### Implementation approach

- Add a new section "Resource Type Reference" after the existing `jobs` and `pipelines` sections
- Expand the Control 5 hallucination whitelist to include the new resource types
- Add cross-references to Skill 02 for `quality_monitors` and Skill 05 for `schemas`

---

## 5. Additional v2 Enhancements

### Testing Instructions

Add a verification component to each skill's output format that includes:
- `EXPLAIN` plan checks for generated SQL (verify no full table scans)
- Assertion patterns for PySpark DataFrames (row count, null count, schema validation)
- Dry-run mode instructions for destructive operations

### Nested Struct Evolution (Skill 05)

Add coverage for `ALTER TABLE ADD COLUMN nested.subfield STRING` and nested struct renaming/dropping via column mapping.

### Join Type Coverage for Salting (Skill 06)

Document the behaviour of key salting with `left`, `right`, and `full outer` join types. Explicitly state that salting does not work with `full outer` joins.

### Version Drift Detection

Add a maintenance schedule to each skill's frontmatter:

```yaml
review_cadence: quarterly
next_review: "2026-09"
api_dependencies:
  - "databricks-sdk >= 0.30.0"
  - "delta-spark >= 3.0.0"
```

---

## 6. Timeline

| Phase | Scope | Target |
|-------|-------|--------|
| v2.0 — Error Handling | Add error patterns to all 6 skills | Q3 2026 |
| v2.1 — Orchestration Skill | Create Skill 07 (pipeline composition) | Q3 2026 |
| v2.2 — Cost Annotations | Add cost callouts to all 6 skills | Q3 2026 |
| v2.3 — Resource Expansion | Expand Skill 01 resource types | Q4 2026 |
| v2.4 — Testing & Verification | Add automated test patterns to all skills | Q4 2026 |
