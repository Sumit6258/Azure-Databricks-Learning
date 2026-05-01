# 🚀 Azure Databricks & PySpark — Complete Enterprise Learning Program

> **Production-level, end-to-end learning curriculum** for Azure Databricks and PySpark.  
> Designed for engineers targeting MNC/MAANG-level Data Engineering roles.

---

## 🎯 What This Repository Covers

This is not a tutorial collection — it is a **complete, job-ready knowledge base** built exactly the way enterprise client projects are structured. Every module mirrors real-world patterns used in banking, e-commerce, telecom, and healthcare data platforms.

---

## 📁 Repository Structure

```
Azure-Databricks-Learning/
│
├── 01-Basics/                              # Azure Databricks Fundamentals
│   ├── 01-workspace-clusters.md           # Workspace, clusters, policies, first notebook
│   ├── 02-dbfs-storage-mounts.md          # DBFS, ADLS mounts, dbutils complete reference
│   ├── 03-databricks-repos-git.md         # Git integration, branching strategy, CI/CD
│   └── 04-databricks-cli-rest-api.md      # CLI commands, REST API Python client
│
├── 02-PySpark/                             # PySpark Deep Dive
│   ├── 01-pyspark-deep-dive.md            # RDD vs DataFrame, transformations, joins, windows, UDFs
│   ├── 02-data-ingestion-patterns.md      # CSV, Parquet, JSON, Avro, JDBC, Kafka, REST APIs
│   ├── 03-advanced-transformations.md     # Arrays, Maps, Structs, date/string functions, pivot
│   └── 04-data-quality-framework.md       # Production DQ engine, rules, quarantine, monitoring
│
├── 03-Delta-Lake/                          # Delta Lake — Very Deep
│   ├── 01-delta-lake-complete.md          # ACID, time travel, MERGE, schema evolution, Medallion
│   ├── 02-delta-live-tables.md            # DLT pipelines, @dlt.expect, declarative ETL
│   └── 03-cdf-liquid-clustering-advanced.md # CDF, Liquid Clustering, constraints, Clone, Delta Sharing
│
├── 04-Optimization/                        # Performance Engineering
│   ├── 01-performance-optimization-complete.md # Partitioning, broadcast, caching, skew, AQE, Z-ordering
│   └── 02-photon-serverless-cost.md       # Photon engine, Serverless compute, full cost playbook
│
├── 05-ADF-Integration/                     # Azure Integration
│   ├── 01-adf-databricks-complete.md      # ADF linked service, Key Vault, Managed Identity, Power BI
│   └── 02-adf-advanced-patterns.md        # Tumbling Window, Event triggers, ForEach, Azure Functions
│
├── 06-Projects/                            # Real-World Capstone Projects
│   ├── project-01-batch-pipeline/
│   │   └── README.md                      # Banking fraud detection — full 4-task production pipeline
│   └── project-02-03-incremental-streaming.md # SCD Type 2 incremental + IoT Auto Loader streaming
│
├── 07-Interview-Prep/                      # Interview Preparation
│   └── 01-complete-interview-guide.md     # 36 Q&As, system design scenarios, coding challenges
│
├── 08-Jobs-Pipelines/                      # Databricks Jobs & Workflows
│   ├── 01-jobs-pipelines-complete.md      # Multi-task workflows, retry, parameterized notebooks, CI/CD
│   └── 02-advanced-streaming-patterns.md  # Stateful streaming, stream-stream joins, Kafka production
│
├── 09-Unity-Catalog/                       # Data Governance
│   └── 01-unity-catalog-complete.md       # 3-level namespace, GRANT/REVOKE, column masking, row filters
│
├── 10-Monitoring-Debugging/                # Observability
│   └── 01-monitoring-debugging-complete.md # Spark UI deep dive, common errors + exact fixes, logging
│
├── 11-Testing/                             # Testing & Code Quality
│   └── 01-unit-testing-pyspark.md         # pytest setup, chispa, 20+ test cases, Delta MERGE tests
│
├── 12-CLI-Terraform/                       # Infrastructure as Code
│   └── 01-terraform-databricks.md         # Full Terraform: workspace, clusters, jobs, Unity Catalog
│
├── 13-Real-World-Patterns/                 # Enterprise Design Patterns
│   └── 01-design-patterns.md              # SCD Type 1/2, watermark+dedup, fan-out, dead letter queue
│
├── 14-Cheatsheets/                         # Quick Reference (Print These!)
│   ├── 01-complete-cheatsheet.md          # Full PySpark API, Delta SQL, dbutils, Spark configs
│   └── 02-interview-by-difficulty.md      # 120 Q&As by level + 5 coding challenges + design template
│
└── 15-SQL-Analytics/                       # SQL & BI Integration
    └── 01-sql-analytics-complete.md        # Advanced SQL patterns, dbt, Power BI optimization
```

---

## 🗺️ Learning Path

```
WEEK 1   → 01-Basics (all 4 files)
           Workspace, clusters, CLI, Git branching workflow

WEEK 2   → 02-PySpark (01 + 02)
           Core PySpark API + every ingestion format (CSV, Parquet, JDBC, Kafka)

WEEK 3   → 02-PySpark (03 + 04) + 03-Delta-Lake (01)
           Advanced transforms, Data Quality framework, Delta core concepts

WEEK 4   → 03-Delta-Lake (02 + 03) + 04-Optimization (01)
           Delta Live Tables, CDF, Liquid Clustering, performance tuning

WEEK 5   → 04-Optimization (02) + 05-ADF-Integration (01 + 02)
           Photon, cost reduction, ADF triggers, Key Vault, Power BI

WEEK 6   → 08-Jobs-Pipelines + 09-Unity-Catalog + 10-Monitoring-Debugging
           Production workflows, governance, Spark UI debugging

WEEK 7   → 11-Testing + 12-CLI-Terraform + 13-Real-World-Patterns
           Unit testing, IaC, SCD patterns, dead letter queues

WEEK 8   → 06-Projects (build all 3 end-to-end — the most important week)
           Batch pipeline, incremental SCD2, streaming Auto Loader

WEEK 9   → 07-Interview-Prep + 14-Cheatsheets + 15-SQL-Analytics
           120 Q&As, system design practice, SQL patterns
```

---

## 🛠️ Prerequisites

| Requirement | Level Needed |
|---|---|
| Python | Intermediate (functions, classes, list comprehensions) |
| SQL | Intermediate (joins, window functions, aggregations) |
| Azure Portal | Basic (can navigate, create resources) |
| Git | Basic (clone, commit, push) |

---

## 💼 Resume Bullet Points (After Completing This Program)

```
• Designed and implemented end-to-end batch and streaming data pipelines on
  Azure Databricks using PySpark and Delta Lake following Medallion Architecture

• Built incremental load pipelines using Auto Loader with schema evolution and
  watermark-based deduplication processing 500K+ daily records

• Optimized Spark jobs reducing processing time by 60% using broadcast joins,
  Z-ordering, partition pruning, and adaptive query execution (AQE)

• Implemented ACID-compliant data lake using Delta Lake with MERGE operations
  for upserts, time travel auditing, schema enforcement, and Change Data Feed
  for incremental downstream propagation

• Integrated Azure Data Factory with Databricks Workflows for orchestration,
  configured Key Vault-backed secrets and Managed Identity for zero-trust access

• Enforced data governance using Unity Catalog with RBAC, row-level security,
  and column masking on PII data across Dev/Test/Prod environments

• Built production data quality framework with severity-based rules, quarantine
  tables, and volume anomaly detection across 10+ validation layers

• Implemented SCD Type 2, fan-out pipelines, dead letter queues, and
  config-driven ETL patterns following enterprise design standards

• Provisioned Databricks infrastructure (workspaces, clusters, jobs, Unity Catalog)
  using Terraform with remote state, modules, and GitHub Actions CI/CD

• Achieved 80%+ unit test coverage for PySpark transformations using pytest
  and chispa, integrated into CI pipeline blocking merges on test failure
```

---

## 📌 Module Index

| # | Module | Files | Difficulty | Key Topics |
|---|--------|-------|-----------|------------|
| 01 | Azure Databricks Basics | 4 | 🟢 Beginner | Workspace, clusters, CLI, Git, REST API |
| 02 | PySpark Deep Dive | 4 | 🟡 Intermediate | All formats, joins, windows, DQ framework |
| 03 | Delta Lake | 3 | 🟡 Intermediate | MERGE, DLT, CDF, Liquid Clustering |
| 04 | Performance Optimization | 2 | 🔴 Advanced | Photon, Serverless, skew, cost |
| 05 | Azure Integration | 2 | 🟡 Intermediate | ADF triggers, Key Vault, Power BI |
| 06 | Projects | 2 | 🔴 Advanced | 3 real pipelines: batch, SCD2, streaming |
| 07 | Interview Prep | 1 | 🟡 All levels | 36 Q&As, coding challenges, system design |
| 08 | Jobs & Pipelines | 2 | 🟡 Intermediate | Workflows, stateful streaming, Kafka |
| 09 | Unity Catalog | 1 | 🟡 Intermediate | Governance, column masking, row filters |
| 10 | Monitoring & Debugging | 1 | 🔴 Advanced | Spark UI, error taxonomy, structured logs |
| 11 | Testing | 1 | 🟡 Intermediate | pytest, chispa, Delta write tests |
| 12 | CLI & Terraform | 1 | 🔴 Advanced | Full IaC for Databricks infrastructure |
| 13 | Real-World Patterns | 1 | 🔴 Advanced | SCD, fan-out, DLQ, config-driven ETL |
| 14 | Cheatsheets | 2 | 🟢 Reference | Full API ref + 120 interview Q&As |
| 15 | SQL Analytics | 1 | 🟡 Intermediate | Advanced SQL, dbt, Power BI tuning |

---

## 📊 Repository Stats

| Metric | Count |
|---|---|
| Total Modules | 15 |
| Total Files | 29 |
| Total Lines of Content | ~14,000+ |
| Interview Q&As | 120+ |
| PySpark Code Examples | 400+ |
| Real Production Projects | 3 |
| Design Patterns Covered | 12+ |

---

## 🔗 Quick Navigation

| I want to... | Go here |
|---|---|
| Start from scratch | [01-Basics/01-workspace-clusters.md](./01-Basics/01-workspace-clusters.md) |
| Understand Delta Lake | [03-Delta-Lake/01-delta-lake-complete.md](./03-Delta-Lake/01-delta-lake-complete.md) |
| Print a reference sheet | [14-Cheatsheets/01-complete-cheatsheet.md](./14-Cheatsheets/01-complete-cheatsheet.md) |
| Prepare for interviews | [14-Cheatsheets/02-interview-by-difficulty.md](./14-Cheatsheets/02-interview-by-difficulty.md) |
| Build my first pipeline | [06-Projects/project-01-batch-pipeline/README.md](./06-Projects/project-01-batch-pipeline/README.md) |
| Learn data quality | [02-PySpark/04-data-quality-framework.md](./02-PySpark/04-data-quality-framework.md) |
| Reduce cluster costs | [04-Optimization/02-photon-serverless-cost.md](./04-Optimization/02-photon-serverless-cost.md) |
| Write tests for PySpark | [11-Testing/01-unit-testing-pyspark.md](./11-Testing/01-unit-testing-pyspark.md) |
| Set up with Terraform | [12-CLI-Terraform/01-terraform-databricks.md](./12-CLI-Terraform/01-terraform-databricks.md) |
| Learn SCD Type 2 | [13-Real-World-Patterns/01-design-patterns.md](./13-Real-World-Patterns/01-design-patterns.md) |

---

## 🚀 How to Use This Repository

```bash
# Clone to your local machine
git clone https://github.com/Sumit6258/Azure-Databricks-Learning.git
cd Azure-Databricks-Learning

# Or clone into Databricks Repos
# Workspace → Repos → Add Repo → paste the URL above
```

**Recommended workflow:**
1. Read the markdown file for the topic
2. Type (don't copy-paste) the code examples into a Databricks notebook
3. Run on a single-node cluster to verify understanding
4. Modify the code — break it, fix it, extend it
5. Complete the project for that module before moving on

---

*Built for engineers who want to work on real enterprise projects — not toy examples.*