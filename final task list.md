# The Top 6 Prioritized Databricks Tasks for SKILL.md Automation

This document outlines the 6 most critical, high-leverage Databricks data engineering tasks to build `SKILL.md` files for. These tasks were selected because they are boilerplate-heavy, highly error-prone, and perfectly align with the core strengths of AI coding agents (enforcing strict syntax, following workflows, and iterating on errors).

---

## 1. Databricks Asset Bundles (DAB) CI/CD Configuration

### Why it is important:
Databricks Asset Bundles (DABs) are the modern standard for deploying infrastructure-as-code on Databricks. They allow teams to define Jobs, Lakeflow Pipelines, and clusters in YAML (`databricks.yml`), and deploy them consistently across `dev`, `staging`, and `prod` environments. Getting this right is critical: a misconfigured bundle might accidentally deploy a massive, expensive production cluster into a sandbox environment, or worse, deploy untested code into production because environment branching was wired incorrectly. Furthermore, failing to configure service principals correctly means jobs will fail silently when the authoring engineer goes on vacation.

### Why a `SKILL.md` file is helpful to engineers:
YAML configuration is notoriously fragile, has strict syntax rules, and lacks intuitive autocomplete. Engineers often copy-paste bundles from old projects, carrying over technical debt or outdated target overrides.
* **The Agentic Workflow:** When an engineer says, *"Create a deployment bundle for my new sales pipeline,"* the AI agent reads the `SKILL.md` file. The skill uses the **Low Freedom & Templates** pattern, providing the agent with the exact canonical structure required by Databricks. 
* **The Value:** Instead of the agent hallucinating Terraform code or legacy `dbx` commands, the skill forces it to generate a perfect, modern `databricks.yml`. It ensures the agent includes mandatory boilerplate like variable substitution (`${var.catalog_name}`), cross-environment target overrides, and service principal `run_as` blocks. The engineer saves hours of frustrating syntax debugging.

---

## 2. Data Quality with SDP Expectations & Lakehouse Monitoring

### Why it is important:
Data quality failures are catastrophic. If a row slips into the Silver layer with a negative transaction amount or a null customer ID, it corrupts downstream Gold tables and executive dashboards. The business loses trust in the data. Implementing quality gates—such as checking for referential integrity, freshness, and regex formats—is the only way to stop this. However, because writing these checks does not immediately produce a new "feature," it is often deferred or skipped entirely.

### Why a `SKILL.md` file is helpful to engineers:
Implementing data quality requires navigating incompatible APIs: Lakeflow Spark Declarative Pipelines (SDP) uses `@dlt.expect`, while standalone jobs use Databricks Lakehouse Monitoring (`CREATE MONITOR`).
* **The Agentic Workflow:** An engineer simply prompts, *"Add data quality checks to ensure customer_id is not null and email is valid."* The agent consults the `SKILL.md`, which is structured using **Domain-Specific Organization**. 
* **The Value:** The skill provides a library of the 15 most common SQL predicate templates (e.g., regex formats, null checks). The agent instantly drops in the correct `@dlt.expect_or_drop("valid_email", "email RLIKE '^[a-zA-Z0-9+_.-]+@[a-zA-Z0-9.-]+$'")` decorator. It removes the cognitive burden of memorizing syntax, turning data quality from a tedious chore into an automated, single-prompt task.

---

## 3. Delta SCD Type 1 & 2 with `MERGE INTO`

### Why it is important:
Handling Slowly Changing Dimensions (SCD) is the beating heart of the Silver layer. When business entities (like customers or products) change over time, the data warehouse must either overwrite the old record (Type 1) or preserve history by expiring the old record and inserting a new one with `valid_from` and `valid_to` timestamps (Type 2). If this is coded incorrectly, pipelines will create duplicate history rows, silently drop updates, or cause massive performance bottlenecks by running full table scans on every micro-batch.

### Why a `SKILL.md` file is helpful to engineers:
Writing a `MERGE INTO` statement from scratch is an error-prone 50-line exercise. Engineers frequently forget critical edge cases, such as using the `null`-safe equality operator (`<=>`) when comparing foreign keys.
* **The Agentic Workflow:** The `SKILL.md` utilizes the **Conditional Workflow & Examples** pattern. The agent asks the engineer if they need Type 1 or Type 2. 
* **The Value:** The skill feeds the agent a flawless, battle-tested `MERGE` template. It guarantees that the agent includes the correct matching predicates, the correct window functions for timestamp boundaries, and instructions to apply `Liquid Clustering` on the merge key. The engineer gets a production-grade SCD pipeline instantly, without the fear of introducing subtle duplicate-row bugs.

---

## 4. Auto Loader Incremental Ingestion

### Why it is important:
Auto Loader (`cloudFiles`) is the standard Databricks mechanism for incrementally streaming raw JSON, CSV, or Parquet files from cloud storage (S3/ADLS) into the Bronze layer. While incredibly powerful, a single misconfiguration can be disastrous. Missing a `checkpointLocation` causes the stream to process the same files multiple times. Not configuring `maxFilesPerTrigger` can cause the cluster to run out of memory (OOM) on its first run when it tries to ingest years of historical data at once.

### Why a `SKILL.md` file is helpful to engineers:
Auto Loader has dozens of options that dictate schema inference, exactly-once semantics, and stream triggering. Engineers shouldn't have to memorize them.
* **The Agentic Workflow:** The skill leverages **Progressive Disclosure**. The agent reads the core Auto Loader configuration and applies the exact `cloudFiles` dictionary needed.
* **The Value:** If an engineer asks, *"Ingest the raw Zendesk JSON bucket,"* the agent knows via the `SKILL.md` to automatically set `cloudFiles.schemaEvolutionMode = "rescue"` to handle messy JSON. It enforces best practices like generating a unique checkpoint path and setting up the `_rescued_data` column. The engineer doesn't have to spend 45 minutes reading Databricks documentation to remember the syntax.

---

## 5. Delta Schema Evolution Management

### Why it is important:
Upstream data sources change constantly. An API might add a new column, or a database migration might rename one. If the Lakehouse isn't prepared, the pipeline will crash, breaking SLAs. Databricks provides tools to handle this (`mergeSchema`, `overwriteSchema`, and column mapping), but they are dangerous. Using `overwriteSchema=True` recklessly forces a complete rewrite of all underlying Parquet files, driving up compute costs and potentially corrupting the table if done incorrectly.

### Why a `SKILL.md` file is helpful to engineers:
Managing schema drift is a high-stakes, stressful operation. Engineers often guess which flag to use.
* **The Agentic Workflow:** The `SKILL.md` acts as a strict safety protocol, utilizing the **Verifiable Intermediate Outputs (Plan-Validate-Execute)** pattern. 
* **The Value:** When an engineer asks to evolve a schema, the agent is instructed by the skill *not* to immediately write code. Instead, it generates a "Schema Migration Plan," checking if the change requires Column Mapping (`delta.columnMapping.mode = 'name'`) or if it's a simple additive change. It prompts the user to review the plan before it executes the `ALTER TABLE` commands. This skill turns the AI agent into a safety guardrail, preventing engineers from accidentally destroying data.

---

## 6. Optimizing Skewed PySpark Joins and Aggregations

### Why it is important:
Data skew is the silent killer of Spark jobs. If 80% of your data maps to a single join key (e.g., `country = 'US'`), one executor takes hours to finish while the others sit idle. This breaches SLAs and wastes massive amounts of cluster compute money. Fixing this requires deep Spark internals knowledge—knowing how to read the Catalyst Optimizer plan, adjusting Adaptive Query Execution (AQE), or implementing manual "key salting" (adding random suffixes to keys to distribute the data).

### Why a `SKILL.md` file is helpful to engineers:
Most engineers are not Spark tuning experts. Diagnosing and salting a skewed join is an advanced heuristic process that takes days of trial and error.
* **The Agentic Workflow:** The `SKILL.md` uses the **High Freedom (Heuristics) & Workflows** pattern. It acts as an expert troubleshooting manual.
* **The Value:** An engineer simply says, *"My customer join job has been hanging at 99% for two hours."* The agent uses the skill to walk through a diagnostic checklist. It first advises the engineer to check the Spark UI to confirm straggler tasks. Then, it automatically generates the complex boilerplate for key salting (broadcasting the smaller table and appending `F.rand()` to the skewed key). The engineer gets elite-level performance tuning instantly, without needing a principal architect in the room.
