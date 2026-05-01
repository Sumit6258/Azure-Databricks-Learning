# Module 01 — Basics
## Part 3: Databricks Repos, Git Integration & Branching Strategy

> In every enterprise project, code lives in Git — not in the personal workspace. This module covers the exact workflow used on real client projects.

---

## 3.1 Why Databricks Repos (Not Workspace Notebooks)?

```
WORKSPACE NOTEBOOKS (Personal Workspace):
  ✗ No version control
  ✗ No peer review (PRs)
  ✗ No rollback on broken code
  ✗ No CI/CD integration
  ✗ Risk of accidental deletion
  ✗ Can't be tested outside Databricks

DATABRICKS REPOS (Git-Connected):
  ✅ Full version control (Git history)
  ✅ Pull request workflows
  ✅ Branch per feature/fix
  ✅ CI/CD triggers on push
  ✅ Unit tests run before merge
  ✅ Multiple developers, no conflicts
```

**Rule on every enterprise project:** All production notebooks live in Repos. Personal workspace is ONLY for quick scratch work.

---

## 3.2 Connecting GitHub to Databricks — Step by Step

### Step 1: Create GitHub Personal Access Token

```
github.com → Settings → Developer Settings
→ Personal Access Tokens → Tokens (classic)
→ Generate New Token (classic)

Scopes to select:
  ✅ repo         (full repo access)
  ✅ workflow     (for GitHub Actions)

Copy the token — you'll only see it once!
```

### Step 2: Add Token to Databricks

```
Databricks Workspace → top-right avatar → User Settings
→ Linked Accounts → Git Integration

Provider: GitHub
Token:    <paste your PAT token>
Username: <your GitHub username>
→ Save
```

### Step 3: Clone a Repo into Databricks

```
Workspace → Repos → Add Repo

URL: https://github.com/Sumit6258/Azure-Databricks-Learning.git
Provider: GitHub (auto-detected)
→ Create Repo

Structure created:
Repos/
└── Sumit6258/
    └── Azure-Databricks-Learning/
        ├── 01-Basics/
        ├── 02-PySpark/
        └── ...
```

---

## 3.3 Git Branching Strategy for Data Engineering

### GitFlow for Data Pipelines

```
Branch Strategy:
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

main ────────────────────────────────────────────→  (Production)
  │
  ├─── release/2024-Q1 ────────────────────────→  (Release candidate)
  │
develop ─────────────────────────────────────────→  (Integration)
  │
  ├─── feature/add-fraud-detection ─────────→  (Your work)
  ├─── feature/silver-schema-update ────────→  (Teammate work)
  ├─── bugfix/null-handling-silver ─────────→  (Bug fix)
  └─── hotfix/prod-pipeline-failure ────────→  (Emergency fix to main)

Rules:
  • Never commit directly to main or develop
  • feature/* branches merge to develop via PR
  • develop merges to main only after QA sign-off
  • hotfix/* branches can go directly to main + develop
```

### Working in Databricks Repos with Branches

```
# In Databricks Repos UI:
Repos → Your Repo → Branch selector (top left of file tree)
→ + (create new branch)
  Name: feature/add-fraud-detection
  From: develop
→ Create

# Now all changes in this branch stay isolated
# Edit notebooks, add new files
# When ready: Repos → Git operations → Commit & Push
  Commit message: "feat: add velocity-based fraud detection rules"
  
# Open PR on GitHub:
  Compare: feature/add-fraud-detection → develop
  Reviewers: team lead
  Description: "Adds 4 fraud rules: velocity, amount anomaly, round amount, high value"
```

### Commit Message Convention (Used on Real Projects)

```
Format: <type>(<scope>): <description>

Types:
  feat     → new feature
  fix      → bug fix  
  refactor → code restructure (no new feature)
  perf     → performance improvement
  docs     → documentation only
  test     → adding/updating tests
  chore    → config, build changes

Examples:
  feat(silver): add SCD Type 2 customer dimension
  fix(bronze): handle null values in amount column
  perf(gold): replace Python UDF with built-in function
  refactor(utils): extract common transform to shared module
  test(silver): add unit tests for amount validation
```

---

## 3.4 Databricks Repos API — Automate Branch Operations

```python
import requests

DATABRICKS_HOST  = "https://adb-workspace.azuredatabricks.net"
DATABRICKS_TOKEN = dbutils.secrets.get("kv-scope", "databricks-pat")

headers = {"Authorization": f"Bearer {DATABRICKS_TOKEN}"}

# List all repos
response = requests.get(
    f"{DATABRICKS_HOST}/api/2.0/repos",
    headers=headers
)
repos = response.json()["repos"]
for repo in repos:
    print(f"Repo: {repo['path']} | Branch: {repo['branch']}")

# Update a repo to a specific branch (used in CI/CD)
repo_id = "123456789"  # Get from list above

response = requests.patch(
    f"{DATABRICKS_HOST}/api/2.0/repos/{repo_id}",
    headers=headers,
    json={"branch": "main"}
)
print(f"Updated: {response.json()}")

# Create a new branch
response = requests.post(
    f"{DATABRICKS_HOST}/api/2.0/repos/{repo_id}/branches",
    headers=headers,
    json={
        "branch": "feature/new-pipeline",
        "tag": None
    }
)
```

---

## 3.5 Project Structure — Production Standard

```
Azure-Databricks-Learning/               ← Root (GitHub repo)
│
├── .github/
│   └── workflows/
│       ├── ci-on-pr.yml                ← Run tests on every PR
│       └── deploy-to-prod.yml          ← Deploy on merge to main
│
├── notebooks/                          ← All Databricks notebooks
│   ├── ingestion/
│   │   ├── 01_ingest_bronze.py
│   │   └── 02_validate_schema.py
│   ├── transformation/
│   │   ├── 03_transform_silver.py
│   │   └── 04_apply_business_rules.py
│   ├── serving/
│   │   └── 05_build_gold.py
│   └── utils/
│       ├── config.py                   ← %run this from every notebook
│       ├── logger.py
│       └── data_quality.py
│
├── src/                                ← Pure Python modules (importable)
│   └── data_platform/
│       ├── __init__.py
│       ├── transformations.py
│       ├── validators.py
│       └── schemas.py
│
├── tests/                              ← Unit + integration tests
│   ├── conftest.py                     ← Shared fixtures
│   ├── unit/
│   │   ├── test_transformations.py
│   │   └── test_validators.py
│   └── integration/
│       └── test_end_to_end.py
│
├── infrastructure/                     ← IaC (Terraform)
│   ├── main.tf
│   ├── variables.tf
│   └── modules/
│       ├── databricks_cluster/
│       └── databricks_job/
│
├── jobs/                               ← Job JSON definitions
│   ├── daily_etl_job.json
│   └── streaming_job.json
│
├── docs/                               ← Architecture diagrams, runbooks
│   ├── architecture.md
│   └── runbook.md
│
├── requirements.txt                    ← Python dependencies for tests
├── setup.py
└── README.md
```

---

## 3.6 GitHub Actions CI/CD Pipeline — Complete

```yaml
# .github/workflows/ci-on-pr.yml

name: CI — Databricks Pipeline Tests

on:
  pull_request:
    branches: [main, develop]
    paths:
      - 'notebooks/**'
      - 'src/**'
      - 'tests/**'

env:
  PYTHON_VERSION: '3.10'
  SPARK_VERSION: '3.4.1'

jobs:
  # ─────────────────────────────────────────────────────────
  # JOB 1: Code Quality
  # ─────────────────────────────────────────────────────────
  lint:
    name: Code Quality Checks
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v3

      - name: Set up Python
        uses: actions/setup-python@v4
        with:
          python-version: ${{ env.PYTHON_VERSION }}

      - name: Install linting tools
        run: pip install flake8 black isort

      - name: Check formatting (Black)
        run: black --check src/ tests/

      - name: Check import ordering (isort)
        run: isort --check-only src/ tests/

      - name: Lint (flake8)
        run: flake8 src/ tests/ --max-line-length=120 --extend-ignore=E203

  # ─────────────────────────────────────────────────────────
  # JOB 2: Unit Tests
  # ─────────────────────────────────────────────────────────
  unit-tests:
    name: Unit Tests (PySpark)
    runs-on: ubuntu-latest
    needs: lint
    steps:
      - uses: actions/checkout@v3

      - name: Set up Python
        uses: actions/setup-python@v4
        with:
          python-version: ${{ env.PYTHON_VERSION }}

      - name: Cache pip packages
        uses: actions/cache@v3
        with:
          path: ~/.cache/pip
          key: ${{ runner.os }}-pip-${{ hashFiles('requirements.txt') }}

      - name: Install dependencies
        run: |
          pip install pyspark==3.4.1 delta-spark==2.4.0 pytest pytest-cov chispa

      - name: Run unit tests
        run: |
          pytest tests/unit/ -v \
            --tb=short \
            --cov=src \
            --cov-report=xml \
            --cov-fail-under=80

      - name: Upload coverage report
        uses: codecov/codecov-action@v3
        with:
          file: coverage.xml

  # ─────────────────────────────────────────────────────────
  # JOB 3: Deploy to TEST (on merge to develop)
  # ─────────────────────────────────────────────────────────
  deploy-test:
    name: Deploy to TEST Environment
    runs-on: ubuntu-latest
    needs: unit-tests
    if: github.ref == 'refs/heads/develop'
    environment: test
    steps:
      - uses: actions/checkout@v3

      - name: Install Databricks CLI
        run: pip install databricks-cli

      - name: Configure CLI
        env:
          DATABRICKS_HOST:  ${{ secrets.DATABRICKS_TEST_HOST }}
          DATABRICKS_TOKEN: ${{ secrets.DATABRICKS_TEST_TOKEN }}
        run: |
          databricks configure --token <<EOF
          $DATABRICKS_HOST
          $DATABRICKS_TOKEN
          EOF

      - name: Update Repos to latest main
        run: |
          databricks repos update \
            --path /Repos/ci-cd/Azure-Databricks-Learning \
            --branch develop

      - name: Trigger test pipeline run
        env:
          DATABRICKS_HOST:  ${{ secrets.DATABRICKS_TEST_HOST }}
          DATABRICKS_TOKEN: ${{ secrets.DATABRICKS_TEST_TOKEN }}
        run: |
          RUN_ID=$(databricks jobs run-now \
            --job-id ${{ secrets.TEST_JOB_ID }} \
            --notebook-params '{"environment":"test","batch_date":"2024-01-15"}' \
            | jq -r '.run_id')
          
          echo "Job run started: $RUN_ID"
          
          # Poll until complete
          while true; do
            STATUS=$(databricks runs get --run-id $RUN_ID | jq -r '.state.life_cycle_state')
            echo "Status: $STATUS"
            if [ "$STATUS" = "TERMINATED" ]; then
              RESULT=$(databricks runs get --run-id $RUN_ID | jq -r '.state.result_state')
              echo "Result: $RESULT"
              [ "$RESULT" = "SUCCESS" ] && exit 0 || exit 1
            fi
            sleep 30
          done

  # ─────────────────────────────────────────────────────────
  # JOB 4: Deploy to PROD (manual trigger on main)
  # ─────────────────────────────────────────────────────────
  deploy-prod:
    name: Deploy to PRODUCTION
    runs-on: ubuntu-latest
    needs: deploy-test
    if: github.ref == 'refs/heads/main'
    environment:
      name: production
      url: https://adf.azure.com/pipelines
    steps:
      - uses: actions/checkout@v3

      - name: Install Databricks CLI
        run: pip install databricks-cli

      - name: Deploy to Production Repos
        env:
          DATABRICKS_HOST:  ${{ secrets.DATABRICKS_PROD_HOST }}
          DATABRICKS_TOKEN: ${{ secrets.DATABRICKS_PROD_TOKEN }}
        run: |
          databricks configure --token <<EOF
          $DATABRICKS_HOST
          $DATABRICKS_TOKEN
          EOF
          
          databricks repos update \
            --path /Repos/ci-cd/Azure-Databricks-Learning \
            --branch main

      - name: Update job definition
        run: |
          databricks jobs reset \
            --job-id ${{ secrets.PROD_JOB_ID }} \
            --json @jobs/daily_etl_job.json

      - name: Send deployment notification
        uses: slackapi/slack-github-action@v1.24.0
        with:
          payload: |
            {
              "text": "✅ Production deployment successful!\nCommit: ${{ github.sha }}\nBy: ${{ github.actor }}"
            }
        env:
          SLACK_WEBHOOK_URL: ${{ secrets.SLACK_WEBHOOK }}
```

---

## 3.7 Interview Questions — Git & Repos

**Q: How do you handle notebook conflicts when multiple engineers work on the same file?**
> Feature branching prevents this — each engineer works in their own `feature/*` branch. Merge conflicts are resolved via PRs before merging to `develop`. For shared utility notebooks (`utils/config.py`), changes require a PR review to prevent conflicts. We also modularize shared logic into Python modules in `src/` which are version-controlled separately from notebooks.

**Q: How do you roll back a bad deployment in Databricks Repos?**
> `git revert <commit-sha>` creates a new commit undoing the bad change. Push to `main`, then `databricks repos update --branch main` to sync Databricks. For immediate emergency rollback: `databricks repos update --tag v1.2.3` to pin to a release tag. Job clusters pick up new code on the next run — no manual intervention needed.

---
*Next: [Part 4 — Databricks CLI & REST API](./04-databricks-cli-rest-api.md)*
