# Strategic SKILL.md Library: 10 High-Value PySpark & Databricks Tasks

## Introduction

The modern data engineering landscape on Databricks is a landscape of immense power — and
immense repetition. Every team that builds on PySpark and Databricks eventually converges on
the same set of hard-won patterns: how to ingest data safely with Auto Loader, how to merge
CDC streams without races or duplicates, how to write tests that actually run without a live
cluster. Yet despite this convergence, the industry still produces this code from scratch on
every project. Engineers copy from Stack Overflow, hallucinate APIs that have since been
deprecated, skip critical production hardening steps, and rediscover the same edge cases their
colleagues solved six months ago on a different team.

This is the problem a `SKILL.md` library solves at its root.

A `SKILL.md` file is a focused, machine-readable playbook that an AI coding agent can detect
and consume at the moment it is about to generate code. Rather than producing a generic, often
naive first draft, the agent instead inherits the collective wisdom of the file: the exact
Databricks Runtime (DBR) APIs to call, the partition strategies that survive skew, the schema
evolution flags that prevent silent data loss, the Unity Catalog governance hooks the security
team requires, and the test patterns that work without a live Spark context.

The 10 tasks below were selected because they satisfy three criteria simultaneously:

1. **Frequency** — They appear in virtually every Databricks data engineering project, from
   startup lakehouses to Fortune 500 enterprise platforms.
2. **Boilerplate density** — A significant fraction of the code is structural scaffolding
   rather than business logic, making it a high-value target for agent-assisted generation.
3. **Risk surface** — Getting them wrong has disproportionate consequences: silent data loss,
   runaway compute costs, failed audits, or broken SLAs.

Together they span the full engineering lifecycle: ingestion, Medallion transformations (Bronze
→ Silver → Gold), CDC, performance tuning, data quality, testing, and CI/CD deployment via
Databricks Asset Bundles.

---

## Task 1: Automated Incremental Ingestion with Auto Loader

### Why it is frequently performed (The Boilerplate Problem)

Auto Loader (`cloudFiles`) is the standard entry point for landing raw files from cloud object
storage (S3, ADLS Gen2, GCS) into the Bronze layer of a Databricks Lakehouse. Despite being
a single Spark source format, using it correctly in production requires a surprisingly dense
set of configuration decisions that engineers routinely get wrong or omit entirely.

The boilerplate burden includes: selecting the correct `cloudFiles.format` and schema
inference mode (`FAILFAST` vs. `PERMISSIVE` vs. `RESCUED_DATA`); configuring a
`checkpointLocation` that is unique per stream to guarantee exactly-once semantics; enabling
`cloudFiles.schemaEvolutionMode` and deciding between `addNewColumns`, `rescue`, and `none`;
setting `cloudFiles.inferColumnTypes` deliberately to avoid the notorious "everything is
a string" trap; managing the `_rescued_data` column for malformed records; and choosing
`trigger(availableNow=True)` vs. `trigger(processingTime="N minutes")` vs. continuous mode
based on SLA requirements — a choice that has direct compute cost implications.

Engineers also routinely forget to set idempotency options (`mergeSchema`, write mode), omit
monitoring for schema evolution events, and fail to size the `maxFilesPerTrigger` or
`maxBytesPerTrigger` options, which can lead to enormous first-run micro-batches that OOM the
cluster. Every new data source in an organization is essentially the same script, written
slightly differently by whichever engineer happened to be free that sprint.

### How a `SKILL.md` file empowers the Agent

A dedicated Auto Loader skill would provide the agent with the canonical, environment-aware
template structure: the precise `readStream` options dictionary with every production flag
documented inline, the correct checkpoint path naming convention (`dbfs:/checkpoints/<catalog>/<schema>/<table>`),
and the decision tree for choosing `schemaEvolutionMode` based on source volatility. It would
encode the `_rescued_data` handling contract — whether to quarantine malformed rows to a
dead-letter table or to fail fast — and provide the idiomatic `writeStream` block with
`foreachBatch` for any merge semantics. The skill would also specify the Databricks-specific
Lakeflow Spark Declarative Pipelines (SDP) alternative using `@dlt.table` with
`spark.readStream.format("cloudFiles")`, giving the agent the knowledge to choose between
the imperative and declarative paradigms based on the engineer's stated context.

---

## Task 2: Delta Lake SCD Type 1 & Type 2 with `MERGE INTO` (Upsert Patterns)

### Why it is frequently performed (The Boilerplate Problem)

Almost every Silver layer table in a Lakehouse represents a business entity — customers,
products, accounts, orders — that changes over time. Implementing Slowly Changing Dimension
(SCD) logic is therefore one of the most universal Silver-layer tasks in all of data
engineering. Yet SCD with `MERGE INTO` on Delta Lake is one of the most error-prone operations
engineers write.

For SCD Type 1 (overwrite), the engineer must correctly define the match predicate, handle
both `WHEN MATCHED THEN UPDATE` and `WHEN NOT MATCHED THEN INSERT` arms, and decide whether
a `WHEN NOT MATCHED BY SOURCE THEN DELETE` clause is needed for full-refresh semantics. For
SCD Type 2 (history preservation), the complexity explodes: the engineer must add `is_current`,
`valid_from`, and `valid_to` surrogate columns; expire existing active rows before inserting
new ones; handle late-arriving records by sorting on a sequence key; manage the risk of
creating duplicate history rows on re-runs if the operation is not idempotent; and correctly
partition and cluster the table to keep merge performance acceptable at scale.

A common mistake is writing the MERGE predicate on a non-clustered column, causing a full
table scan on every run — a subtle performance bug that only manifests weeks later when the
table grows to hundreds of millions of rows. Engineers also frequently forget to handle the
`null`-safe equals operator (`<=>`) in match conditions, producing missed merges on nullable
keys.

### How a `SKILL.md` file empowers the Agent

The skill would give the agent the canonical SCD1 and SCD2 `MERGE INTO` templates in both SQL
and the PySpark `DeltaTable.merge()` fluent API, annotated with the exact conditions that must
be present in each arm. It would encode the surrogate column naming convention, the correct
window function pattern for generating `valid_from`/`valid_to` boundaries, and the
idempotency strategy (using a sequence/timestamp column as the guard). The skill would also
include the liquid clustering recommendation — instructing the agent to issue
`ALTER TABLE ... CLUSTER BY (merge_key)` after creation — and the `optimizeWrite` and
`autoCompact` table properties that should be set at creation time for any high-frequency merge
target. Critically, it would carry the warning about `null`-safe equality, preventing the
agent from generating a predicate that silently fails on nullable foreign keys.

---

## Task 3: Declarative CDC Pipeline Authoring with Lakeflow SDP `AUTO CDC`

### Why it is frequently performed (The Boilerplate Problem)

Change Data Capture from operational databases (Debezium, Fivetran, AWS DMS, Qlik) is the
most common ingestion pattern for enterprise data pipelines. The raw CDC stream lands in the
Bronze layer as a sequence of `INSERT`, `UPDATE`, and `DELETE` events, often with a
`_op` or `__op` column and an `_ts_ms` (timestamp) ordering column. Correctly applying
these events to a Silver materialized table using imperative SQL `MERGE` statements requires
engineers to independently solve ordering (out-of-order events break history), deduplication
(reprocessed micro-batches create phantom rows), partial-update semantics (column-level
patches from UPDATE events), schema evolution (new columns from the source must propagate),
and SCD Type 2 history preservation — each of which is its own multi-day engineering effort.

The resulting imperative code is typically 150–400 lines of PySpark or SQL, is highly
stateful, and is notoriously difficult to unit test. Engineers copy it between projects,
introducing subtle bugs each time they adapt it to a new source's column naming conventions.

Lakeflow SDP's `AUTO CDC` flow (`APPLY CHANGES INTO` in SQL, `apply_changes()` in Python)
solves all of this declaratively, but the API has its own nuances: the difference between
`APPLY AS DELETE WHEN` and `APPLY AS TRUNCATE WHEN` clauses, the `sequence_by` column
requirement, the `column_list` vs. `except_column_list` for partial updates, and the
mandatory `keys` parameter that defines the primary key of the target table. Agents without
this context will default to generating the imperative MERGE pattern, wasting hundreds of
lines and introducing all the manual failure modes.

### How a `SKILL.md` file empowers the Agent

The skill would document the complete `apply_changes()` Python function signature with every
parameter explained, the decision matrix for choosing between `stored_as_scd_type_1` and
`stored_as_scd_type_2`, and the correct way to express `APPLY AS DELETE WHEN` for hard
deletes vs. soft deletes. It would map common CDC source conventions (Debezium `__op` values
of `c`, `u`, `d`, `r`) to the correct SDP clauses, so the agent can generate source-aware
code without hallucinating column names. The skill would also encode the recommended
Bronze → Silver topology: streaming table for the raw CDC feed, `AUTO CDC` flow for the
Silver materialized SCD table, ensuring the agent produces the idiomatic two-layer pattern
rather than collapsing ingestion and transformation into one error-prone monolith.

---

## Task 4: Delta Lake Table Optimization — Liquid Clustering, VACUUM, and OPTIMIZE

### Why it is frequently performed (The Boilerplate Problem)

Delta Lake tables degrade in query performance over time as small files accumulate from
frequent streaming writes and merge operations. Maintaining them requires a recurring
maintenance routine involving `OPTIMIZE` (file compaction), `VACUUM` (removing expired
snapshots), and a clustering strategy that ensures the Parquet file layout matches the query
access pattern. This is a task performed on every production Delta table in an organization,
often scheduled as a nightly Databricks Workflow job.

The boilerplate complexity is significant. Engineers must choose between the legacy
`ZORDER BY` approach (static, requires full rewrites) and the modern Liquid Clustering approach
(`ALTER TABLE ... CLUSTER BY`, self-tuning and incremental). They must set the correct Delta
table properties: `delta.autoOptimize.optimizeWrite`, `delta.autoOptimize.autoCompact`,
`delta.targetFileSize`, and `delta.logRetentionDuration`. They must understand that `VACUUM`
with a retention period shorter than 7 days requires explicitly overriding the safety check
and risks breaking active readers. They must account for the fact that running `OPTIMIZE` on
a table that is being actively written to by a streaming job can cause transaction conflicts,
requiring retry logic. Finally, they must know that Predictive Optimization (a Databricks
Premium feature) can automate this entirely for Unity Catalog managed tables, making the
manual `OPTIMIZE` job redundant in those environments.

### How a `SKILL.md` file empowers the Agent

The skill would encode the decision tree: Unity Catalog managed table with Predictive
Optimization enabled → no manual `OPTIMIZE` job needed; external table or legacy HMS →
generate the maintenance notebook. For the maintenance notebook path, it would provide the
canonical `OPTIMIZE ... WHERE` predicate pattern (partition predicate pushdown for incremental
optimization), the correct `VACUUM` invocation with retention logging, and the full set of
recommended `ALTER TABLE SET TBLPROPERTIES` statements for any new Delta table. The skill
would explicitly flag that `ZORDER BY` and `CLUSTER BY` are mutually exclusive and that
changing from one to the other requires a full table rewrite, preventing the agent from
generating contradictory DDL. It would also include the `DESCRIBE HISTORY` query that
engineers should include in their monitoring jobs to detect optimization regressions.

---

## Task 5: PySpark Unit Testing with `pytest` and Chispa (Without a Live Spark Context)

### Why it is frequently performed (The Boilerplate Problem)

Testing PySpark transformation logic is a persistent bottleneck because most data engineers
default to running full notebook end-to-end, using production or staging clusters, which is
slow, expensive, and does not integrate with CI/CD pipelines. The correct pattern — isolating
transformation functions and testing them with a local `SparkSession` or the `chispa` DataFrame
assertion library — requires a project structure and a set of conventions that most teams
never establish because no one wrote them down.

The boilerplate involved spans multiple layers: creating a `conftest.py` with a module-scoped
`SparkSession` fixture to avoid per-test cluster startup overhead; structuring transformation
code as pure functions that accept and return `DataFrame` objects (rather than reading/writing
inside the function, making them untestable); writing `chispa.assert_df_equality()` calls
with the correct options (`ignore_row_order=True`, `ignore_nullable=True`); mocking Unity
Catalog reads with local Parquet or in-memory `createDataFrame()` calls; and configuring
`pyproject.toml` or `setup.cfg` for `pytest` with the correct `spark.sql.warehouse.dir` and
`derby.system.home` temp paths to prevent test isolation failures when running in parallel.
Engineers routinely spend 1–2 days debugging `SparkSession` fixture contamination bugs that
a correct `conftest.py` would have prevented.

### How a `SKILL.md` file empowers the Agent

The skill would give the agent the canonical `conftest.py` template with the module-scoped
`SparkSession` fixture, the recommended `pytest` configuration block, and the idiomatic
function signature for a testable PySpark transformation (pure `DataFrame → DataFrame`). It
would provide the `chispa` assertion patterns for common scenarios: equality with unordered
rows, schema comparison, and partial column assertion (checking only the columns relevant to
the transformation). The skill would encode the project layout convention — `src/`, `tests/`,
`notebooks/` — that enables agents like Claude Code to correctly place generated test files
relative to source files. It would also document the Databricks `dbutils.fs` mock pattern,
allowing the agent to generate tests for code that calls `dbutils` without requiring an
actual Databricks Runtime.

---

## Task 6: Gold Layer Aggregation — Pre-Aggregated Materialized Views and Semantic Models

### Why it is frequently performed (The Boilerplate Problem)

The Gold layer is where data is shaped for direct consumption by BI tools, dashboards, and
ML feature stores. This means writing aggregation logic (GROUP BY, window functions, pivots)
over large Silver tables, materializing the results as Delta tables or Databricks SQL
Materialized Views, and scheduling refreshes that are fast enough to meet SLA requirements.
Every business domain in an organization — sales, finance, logistics, product — has its own
Gold layer tables. They are the most frequently created artifacts in a mature Lakehouse and the
most commonly written from scratch each time.

The boilerplate problems are layered. Engineers must decide between a SQL Materialized View
(managed by Databricks SQL, automatic incremental refresh) and an SDP Materialized View
(managed by Lakeflow SDP, incremental where possible) — a choice with significant implications
for cost, latency, and governance. They must correctly implement window functions
(`ROW_NUMBER() OVER PARTITION BY`) for deduplication before aggregation to prevent double-
counting. They must apply row-level security (RLS) and column-level masking via Unity Catalog
dynamic views when the Gold table contains PII or financially sensitive data. They must set
`delta.columnMapping.mode = name` for schema evolution compatibility. And they must configure
the table for optimal BI query performance: Liquid Clustering on the BI tool's primary filter
columns (typically `date`, `region`, `product_category`), `ANALYZE TABLE` statistics, and
the `PREDICTIVE_OPTIMIZATION` table property.

### How a `SKILL.md` file empowers the Agent

The skill would encode the decision matrix between SDP Materialized Views, Databricks SQL
Materialized Views, and persisted Delta tables, with the latency/cost/governance tradeoff for
each. It would provide the canonical Gold table DDL template with the full set of `TBLPROPERTIES`
for BI optimization, the dynamic view pattern for RLS and column masking using
`is_account_group_member()`, and the correct `ANALYZE TABLE ... COMPUTE STATISTICS FOR COLUMNS`
invocation. The skill would also include the idiomatic window-function deduplication pattern
as a mandatory pre-aggregation step, preventing the agent from generating a Gold table that
silently double-counts due to upstream duplicates — a class of bug that is extremely difficult
to detect in production and typically manifests as a business escalation rather than a
pipeline failure.

---

## Task 7: Data Quality Validation with SDP Expectations and Lakehouse Monitoring

### Why it is frequently performed (The Boilerplate Problem)

Data quality failures are one of the most expensive recurring problems in data engineering.
A row that slips through with a null primary key, a negative transaction amount, or a
reference to a non-existent dimension record corrupts downstream Gold tables and BI reports
in ways that may not be detected for days or weeks. Every pipeline should have data quality
gates, yet most teams implement them inconsistently or not at all because writing quality
checks is perceived as ancillary work with no immediate business deliverable.

The boilerplate involves multiple layers of the stack. Within an SDP pipeline, the engineer
must use `@dlt.expect()`, `@dlt.expect_or_drop()`, and `@dlt.expect_or_fail()` decorators
with the correct SQL predicate strings, and must decide — for each rule — between warn,
drop, and fail semantics based on the downstream impact of the violation. Outside of SDP, for
standalone PySpark jobs, the engineer must choose between custom assertion code,
`Spark Expectations` (Nike's open-source library), or Databricks' native `Lakehouse Monitoring`
(`CREATE MONITOR`) API. Setting up Lakehouse Monitoring requires specifying the `timeseries_key`,
the `slicing_exprs` for subgroup analysis, the `output_schema_name` for the metrics Delta
table, and an alert threshold configuration. Each of these APIs is different, incompatible,
and poorly understood without deep familiarity with the Databricks platform. Agents without
guidance will default to writing ad-hoc `assert` statements that do not integrate with any
monitoring infrastructure.

### How a `SKILL.md` file empowers the Agent

The skill would provide the complete SDP expectations taxonomy: the three decorators, their
semantic differences, and a decision guide mapping violation severity to the correct decorator.
It would include a canonical library of the 15 most common data quality rule patterns —
null checks, range checks, referential integrity, format validation (regex), uniqueness, and
freshness — expressed as SQL predicate strings ready to drop into `@dlt.expect()` calls. For
non-SDP contexts, it would document the `CREATE MONITOR` DDL with all required parameters
and the recommended alert query pattern. The skill would also encode the data contract
pattern: a YAML or JSON schema that the agent can read from the repository to auto-generate
the expectation decorators from the contract definition, making quality checks a first-class
artifact derived from specification rather than handwritten boilerplate.

---

## Task 8: Optimizing Skewed PySpark Joins and Aggregations

### Why it is frequently performed (The Boilerplate Problem)

Data skew is the single most common root cause of slow or failing Spark jobs in production.
When a join or `GROUP BY` key has a highly uneven distribution — a handful of values
representing a disproportionate fraction of rows (e.g., a `country_code = 'US'` that
accounts for 80% of transactions) — Spark assigns those rows to a single task executor. The
result is a "straggler task" that runs for hours while all other tasks have long since
completed, effectively serializing what should be a parallel computation.

Diagnosing and fixing skew requires knowledge that most engineers do not have readily
available: reading the Spark UI's Stage Detail and Task Metrics tabs to identify the skewed
partition; knowing the difference between a skewed join (requiring AQE skew join optimization
or manual key salting) and a skewed aggregation (requiring a two-stage `map-side pre-aggregate
+ reduce` pattern); correctly configuring Adaptive Query Execution (AQE) with
`spark.sql.adaptive.enabled = true`, `spark.sql.adaptive.skewJoin.enabled = true`, and the
`spark.sql.adaptive.skewJoin.skewedPartitionThresholdInBytes` threshold. For cases where AQE
is insufficient, engineers must implement manual key salting: adding a random bucket suffix
to the skewed key, performing the join against a replicated broadcast of the smaller table,
then aggregating away the salt — a three-step pattern with a specific set of PySpark function
calls that is rarely written correctly on the first attempt. Lakeflow SDP's recommendation to
use Liquid Clustering on the source table also reduces skew at the storage layer, but this
must be coordinated with the runtime AQE settings.

### How a `SKILL.md` file empowers the Agent

The skill would encode the skew diagnosis checklist (what to look for in the Spark UI and
in `EXPLAIN FORMATTED` output), the AQE configuration block with the three key properties
and their recommended values for different table size tiers, and the complete key-salting
PySpark template: the `F.concat(col("key"), F.lit("_"), (F.rand() * N).cast("int"))` salting
step, the join on the salted key against the broadcast-replicated dimension, and the
`groupBy(original_key).agg(...)` desalting step. It would include the broadcast threshold
decision rule (`spark.sql.autoBroadcastJoinThreshold`, defaulting to 10MB, and when to
override it) so the agent knows when to recommend a broadcast hint (`F.broadcast(df)`) versus
the salting approach. This converts a multi-day expert debugging exercise into an agent-guided
code generation task.

---

## Task 9: Databricks Asset Bundles (DAB) CI/CD Pipeline for Lakeflow Jobs and SDP Pipelines

### Why it is frequently performed (The Boilerplate Problem)

Productionizing a Databricks notebook or SDP pipeline means configuring a Lakeflow Job
(formerly Databricks Workflow), deploying it across `dev`, `staging`, and `prod` environments
with environment-specific configuration (cluster sizes, catalog names, notification recipients),
and wiring it into a CI/CD pipeline (GitHub Actions, Azure DevOps, GitLab CI). Databricks
Asset Bundles (DAB) is the standard Infrastructure-as-Code solution for this, replacing the
older Terraform and `dbx` approaches. Yet `databricks.yml` bundle configuration files have a
steep learning curve with a non-obvious syntax and several environment-specific gotchas.

The boilerplate spans the entire file: the top-level `bundle` block with the `name` field;
the `artifacts` block for Python wheel packaging; the `targets` block with per-environment
variable overrides for `catalog`, `schema`, cluster `node_type_id`, and
`num_workers`; the `resources.pipelines` block with the SDP pipeline definition including
`serverless`, `catalog`, `target`, `libraries`, and `configuration` keys; the
`resources.jobs` block that orchestrates multiple tasks (SDP pipeline task + notebook task
+ DQ validation task) with correct `depends_on` wiring; and the `permissions` block for
service principal assignment. Engineers routinely produce bundles that deploy to `prod` with
`dev` cluster sizes, that lack service principal permissions causing silent job failures, or
that omit the `run_as` service principal, making audit lineage impossible. The GitHub Actions
workflow that runs `databricks bundle deploy` also has standard steps — workspace host
configuration, service principal auth via environment variables, environment-based branching
— that every team reimplements.

### How a `SKILL.md` file empowers the Agent

The skill would provide the canonical `databricks.yml` template for a three-environment
Medallion pipeline bundle: the full `targets` block with `dev`/`staging`/`prod` overrides
using DAB variable substitution syntax (`${var.catalog_name}`), the `resources.pipelines`
block for an SDP pipeline with all required fields, and the `resources.jobs` block for the
orchestrating Workflow. It would encode the service principal `run_as` pattern and the
`permissions` block structure, and would provide the canonical GitHub Actions YAML for
`databricks bundle deploy` with the correct secret references and environment-gate steps
(deploy to `prod` only on merge to `main`). The agent, armed with this skill, can generate
a complete, deployable DAB configuration from a description of the pipeline topology in
approximately the same time it takes to write a single notebook — eliminating days of
`databricks.yml` syntax debugging.

---

## Task 10: Schema Evolution Management in Delta Lake (Schema Merge, Enforcement, and Column Mapping)

### Why it is frequently performed (The Boilerplate Problem)

Schema evolution — the process of safely handling changes in the structure of incoming data
(new columns, renamed columns, type widening) without breaking downstream pipelines — is a
recurring and high-stakes task in every live data engineering system. Sources change their
schemas without warning: an upstream API adds a new field, a database migration renames a
column, a Kafka producer starts emitting a new nested struct. If the pipeline does not handle
this correctly, it either fails hard (breaking the SLA) or, worse, silently drops or
misaligns data (corrupting the Gold layer without any visible error).

Delta Lake provides the mechanisms to handle this — `mergeSchema`, `overwriteSchema`,
`delta.columnMapping.mode`, and `ALTER TABLE ... ADD COLUMN / RENAME COLUMN` — but using
them correctly requires understanding a non-obvious set of rules. `mergeSchema=True` on a
write will add new columns but will not rename or drop existing ones. `overwriteSchema=True`
replaces the entire schema but also rewrites all data files, making it expensive and
dangerous on large tables. Column renaming requires `delta.columnMapping.mode = name` to be
set on the table before the first rename, and cannot be retrofitted to an existing table
without a `REWRITE DATA` operation. Auto Loader's `cloudFiles.schemaEvolutionMode = "addNewColumns"`
will evolve the schema but will emit a stream restart event that the engineer must explicitly
handle. In SDP pipelines, the `evolve_schema=True` flag on a streaming table handles schema
evolution declaratively, but only for additive changes; breaking changes still require
explicit pipeline restarts and schema migration steps. Engineers who are unaware of these
distinctions routinely choose the wrong mechanism and either cause data loss or introduce
weeks of downstream schema drift.

### How a `SKILL.md` file empowers the Agent

The skill would provide the definitive schema change taxonomy: additive changes (new columns)
vs. non-breaking type changes (int → long) vs. breaking changes (column rename, column drop,
type narrowing), and the correct Delta Lake mechanism for each category. It would encode the
column mapping enablement DDL (`ALTER TABLE SET TBLPROPERTIES ('delta.columnMapping.mode' = 'name',
'delta.minReaderVersion' = '2', 'delta.minWriterVersion' = '5')`) and the warning that this
must be set at table creation time for full rename support. It would provide the Auto Loader
`schemaEvolutionMode` decision tree and the SDP `evolve_schema` flag usage. For breaking
changes, the skill would supply the schema migration runbook template: the `ALTER TABLE ADD
COLUMN` + backfill pattern, the `CAST` expression for type widening, and the downstream
impact analysis query using Unity Catalog lineage APIs to identify all downstream tables and
jobs that will be affected by the schema change before it is applied. This converts what is
frequently a production incident into a planned, agent-generated migration with full impact
visibility.

---

## Summary Table

| # | Task Name | Lifecycle Stage | Primary Risk Without Skill |
|---|-----------|----------------|---------------------------|
| 1 | Auto Loader Incremental Ingestion | Bronze / Ingestion | Silent data loss, OOM on first run |
| 2 | Delta SCD Type 1 & 2 with MERGE | Silver / Entity History | Duplicated history, null-key misses |
| 3 | Lakeflow SDP `AUTO CDC` Declarative Pipelines | Silver / CDC | 400-line imperative bugs, out-of-order events |
| 4 | Delta Table Optimization (Liquid Clustering, VACUUM) | All Layers / Ops | Runaway storage costs, stale file layouts |
| 5 | PySpark Unit Testing with pytest & Chispa | Testing / CI | No CI coverage, cluster-dependent tests |
| 6 | Gold Aggregation — Materialized Views & Semantic Layer | Gold / Analytics | Double-counting, missing RLS, BI query slowness |
| 7 | Data Quality with SDP Expectations & Lakehouse Monitoring | All Layers / Quality | Silent bad data reaching business consumers |
| 8 | Skewed Join & Aggregation Optimization | Performance Tuning | Straggler tasks, SLA breach, runaway compute |
| 9 | Databricks Asset Bundles CI/CD for Jobs & Pipelines | Deployment / DevOps | Wrong env config in prod, missing audit trails |
| 10 | Delta Schema Evolution Management | All Layers / Governance | Silent data loss or pipeline failure on source change |

---

*Document authored for internal SKILL.md library strategy. Tasks validated against Databricks
official documentation (June 2025), Lakeflow Spark Declarative Pipelines best practices, and
practitioner patterns observed across enterprise Lakehouse implementations.*
