# EPL Data Pipeline - DE Zoomcamp 2026 Final Project

## Table of Contents

- [Overview](#overview)
- [Problem Statement](#problem-statement)
- [Architecture](#architecture)
- [Technologies Used](#technologies-used)
- [Dataset](#dataset)
- [Project Structure](#project-structure)
- [Data Pipeline Explained](#data-pipeline-explained)
- [dbt Models](#dbt-models)
- [Dashboard](#dashboard)
- [How to Reproduce](#how-to-reproduce)
- [Lessons Learned](#lessons-learned)

---

## Overview

This project is the capstone for the Data Engineering Zoomcamp 2026 course. It builds a complete, end-to-end data pipeline that collects English Premier League (EPL) match and player statistics data, stores and transforms it in the cloud, and presents it through an interactive dashboard.

What does "end-to-end data pipeline" mean?
Think of it like a factory assembly line for data: raw, messy football data goes in one end, and clean, analysis-ready tables come out the other end automatically. This project builds every stage of that assembly line.

The entire pipeline is automated, reproducible, and deployed on Google Cloud Platform (GCP).

---

## Problem Statement

Football data is publicly available, but it is scattered across many sources and in raw, unprocessed form. To answer questions like:

- Which team has the best home record this season?
- Who are the top scorers and assist-makers across 9 seasons?
- How has a team's performance evolved across matchweeks?

...you need a structured data pipeline that downloads, cleans, models, and organizes the raw data into a format ready for analysis.

This project builds exactly that, from raw CSV files all the way to a visual dashboard, using modern, industry-standard data engineering tools.

---

## Architecture

The pipeline follows the ELT (Extract, Load, Transform) pattern:

What is ELT?
- Extract: Read the raw data from its original source (CSV files from Kaggle).
- Load: Upload that raw data into cloud data warehouse as-is, without changing anything yet.
- Transform: Once the data is safely in the cloud, run SQL queries to clean and reshape it.

This is different from the older ETL approach where you clean data before loading it. ELT is preferred in modern data engineering because cloud storage is cheap, and it lets you re-run transformations without re-downloading data.

Data Source (CSV and JSON files from Kaggle)
        |
        v
  Python Script (Ingestion Layer running on your local machine)
  - Reads raw EPL CSV and JSON files using Pandas
  - Uploads them directly into BigQuery as raw tables
        |
        v
  BigQuery: "epl_raw" dataset (Data Warehouse - Raw Layer)
  - Raw tables exactly as loaded from the files
  - No cleaning or transformation applied yet
        |
        v
  dbt (Transformation Layer)
  - Staging models: rename columns, fix data types, filter bad rows
  - Mart models: build final analytics-ready tables
  - Outputs go into BigQuery: "epl_core" dataset
        |
        v
  Looker Studio (Dashboard)
  - Connects directly to BigQuery "epl_core" dataset
  - Serves interactive visual reports. No data is exported or moved.

All cloud infrastructure (BigQuery datasets) is provisioned with Terraform, meaning it can be created and destroyed with a single command, and anyone can recreate the exact same environment.

Special note on Architecture Changes:
Originally, this project planned to use Google Cloud Storage (GCS) as a Data Lake and Kestra for Orchestration. However, due to Google Cloud billing account limitations (Google returning 403 Forbidden errors when attempting to create storage buckets on unverified accounts), the architecture was creatively adapted. We bypass GCS completely and use Python BigQuery Client APIs to stream data directly into the Data Warehouse. This ensures the project remains 100% functional without encountering billing blockers.

---

## Technologies Used

| Tool | Category | Purpose in This Project |
|---|---|---|
| Terraform | Infrastructure as Code | Provision the two BigQuery datasets on GCP |
| Python (Pandas + google-cloud-bigquery) | Data Ingestion | Extract data from local files and push directly to BigQuery |
| BigQuery | Data Warehouse | Store, query, and serve transformed data |
| dbt (data build tool) | Transformation | Clean and model raw data into analytics-ready tables |
| Looker Studio | Visualization | Build interactive dashboards connected to BigQuery |
| Git + GitHub | Version Control | Track code changes and host the project publicly |

Why these tools?

- Terraform ensures the infrastructure is version-controlled and reproducible. Anyone who clones this repo can spin up the exact same GCP environment by running two commands. No manual clicking in the GCP console required.
- Python is incredibly flexible for manipulating local data forms. Using Pandas with standard BigQuery libraries allowed us to bypass strict Cloud Storage limitations quickly.
- BigQuery is Google's fully managed, serverless data warehouse. It can query terabytes of data in seconds with no infrastructure to manage. You pay only for what you query.
- dbt applies software engineering best practices (testing, documentation, version control) to SQL transformations. Instead of running SQL manually, dbt tracks every transformation as code.
- Looker Studio connects natively to BigQuery and allows building dashboards without writing code. It queries BigQuery live, so the dashboard always reflects the latest data.

---

## Dataset

Sources:
- Kaggle: Premier League Data from 2016 to 2024
- Kaggle: EPL 2024-2025 Detailed Match Data

Seasons covered: 9 EPL seasons (2016/2017 through 2024/2025)

Scale: Each season contains 380 matches (20 teams x 38 matchweeks), giving approximately 3,400 match records across all seasons combined.

What the data contains:

| Data Type | Fields | Source File Format |
|---|---|---|
| Match results | Home team, away team, home goals, away goals, date, stadium | JSON Arrays |
| Player statistics| Player name, team, goals, assists, appearances, season | CSV |
| League standings | Team, points, wins, draws, losses, goals scored | CSV |
| Club statistics | Overall team performance indicators per season | CSV |

---

## Project Structure

```text
DE Zoomcamp 2026 Final Project/
|
|-- terraform/                  # Infrastructure as Code
|   |-- main.tf                 # Defines 2 BigQuery datasets (epl_raw, epl_core)
|   |-- variables.tf            # Input variables (project ID, region, etc.)
|   |-- terraform.tfvars        # Your actual variable values (NOT committed to Git)
|
|-- load_to_bigquery.py         # Python script to ingest CSVs & JSON to BigQuery
|
|-- epl_dbt/                    # dbt project (data transformation)
|   |-- dbt_project.yml         # dbt project configuration (project name: epl_dbt)
|   |-- models/
|       |-- staging/            # Layer 1: clean raw data from epl_raw dataset
|       |   |-- stg_matches.sql
|       |   |-- stg_players.sql
|       |   |-- stg_club_stats.sql
|       |   |-- stg_standings.sql
|       |   |-- schema.yml      # Column descriptions + automated data tests
|       |-- marts/              # Layer 2: business-ready tables in epl_core dataset
|           |-- dim_players.sql
|           |-- fct_matches.sql
|           |-- fct_standings.sql
|           |-- dim_teams.sql
|           |-- schema.yml
|
|-- datasets 9 seasons/         # Local copies of raw CSV/JSON files
|
|-- epl-data-pipeline-9999-de6a9c38ce47.json   # GCP Service Account key
|-- .gitignore                  # Excludes credentials, terraform state files
|-- README.md                   # This file
```

Important note about BigQuery datasets:
This project uses two separate BigQuery datasets, not one:
- epl_raw: where Python scripts load the raw CSV/JSON data (untouched)
- epl_core: where dbt writes the cleaned, transformed tables (staging views + mart tables)

This separation is intentional: it keeps raw data isolated from transformed data, so you can always go back to the source.

---

## Data Pipeline Explained

This section walks through exactly what happens, step by step, when the pipeline runs.

### Step 1 - Infrastructure Setup (Terraform)

Before any data can flow, the cloud infrastructure must exist. Terraform creates the required GCP resources by reading the configuration in `terraform/main.tf`.

Running `terraform apply` inside the `terraform/` directory creates:
1. A BigQuery dataset named `epl_raw` in the region.
2. A BigQuery dataset named `epl_core` in the same region.

### Step 2 - Data Ingestion (Python)

We execute `load_to_bigquery.py`. This script:
1. Opens each CSV and JSON file locally from the `datasets 9 seasons` directory.
2. Formats them dynamically into Pandas DataFrames.
3. Authenticates directly to Google Cloud using the Service Account JSON Key.
4. Performs an append-or-replace stream directly into the `epl_raw` dataset inside BigQuery.

### Step 3 - Data Transformation (dbt)

Once raw data is in BigQuery's `epl_raw` dataset, dbt takes over. dbt runs SQL models in two layers, and the results go into the `epl_core` dataset.

Staging layer (creates views - not stored as physical tables, recomputed on query):
- Reads from raw BigQuery tables in `epl_raw`.
- Renames columns to consistent, readable conventions (for example, HG becomes home_goals).
- Combines disparate column string spaces using backticks to match the raw schema properly.

Mart layer (creates physical tables stored in BigQuery):
- Reads from staging views.
- Builds final analytics-ready tables:
  - `dim_players`: one row per player per season.
  - `fct_matches`: one row per match, containing full match details.
  - `fct_standings`: aggregated standings.
  - `dim_teams`: aggregated club statistics.

dbt also runs automated data quality tests to catch problems before they reach the dashboard.

### Step 4 - Dashboard (Looker Studio)

Looker Studio connects directly to the `dim_players` and `fct_matches` tables in BigQuery's `epl_core` dataset. No data is exported or copied. The dashboard queries BigQuery live every time it loads.

---

## dbt Models

### Staging Models (stored as views in `epl_core`)

Views are not pre-computed tables. They are saved SQL queries. Every time something queries a view, BigQuery runs the SQL behind it on the spot. This keeps storage costs low.

| Model | Reads From (in epl_raw) | What It Does |
|---|---|---|
| stg_matches | raw_matches_2425 | Cleans match results, standardizes column names, casts data types |
| stg_players | raw_player_stats | Cleans player stats, ensures numeric columns are integers |
| stg_standings | raw_league_table | Cleans league table data |
| stg_club_stats| raw_club_stats | Cleans club statistics data |

### Mart Models (stored as physical tables in `epl_core`)

These are the final tables that the dashboard connects to. They are materialized as actual BigQuery tables, so queries against them are fast.

| Model | Built From | Description |
|---|---|---|
| dim_players | stg_players | One row per player per season - goals, assists, team |
| fct_matches | stg_matches | One row per match - teams, score, matchweek, date, result |
| dim_teams | stg_club_stats | One row per team - performance metrics per season |
| fct_standings | stg_standings | Row-based tournament points table |

### Data Tests

dbt runs validation tests automatically every time you run `dbt test`. All 35 tests must pass before the data is considered valid.

| Test | What It Checks |
|---|---|
| Not Null | Ensures rows are populated with data |
| Unique | Ensures Primary Keys are highly unique |
| Accepted Values | Checks that results strictly match H, D, or A (Home, Draw, Away) |

---

## Dashboard Strategy

The dashboard is built in Looker Studio and is designed with an intuitive three-page structure. A well-designed dashboard is crucial for a data engineering project evaluation.

To impress reviewers, the dashboard incorporates:
- Interactive Filters: A global date range control and a team dropdown filter that applies to all charts.
- Custom Theming: A professional dark theme matching Premier League aesthetics.
- Text Annotations: Small text boxes explaining the data pipeline lineage so reviewers know the data was transformed via dbt.

Page 1: League Overview
   - Features Scorecards tracking fundamental metrics: Games Played, Total Goals, and Avg Goals Per Game.
   - Contains exactly what fans want to see first: A comprehensive League Table ranking the best teams by points across 9 seasons.
   - Highlights the "Top 5 Attacking & Defensive Lineups" using a comparative Bar Chart for Goals For vs Goals Against.

Page 2: Team Analytics
   - Dives deep into tactical team performances using the dim_teams table.
   - Analyzes attacking efficiency with a Scatter Plot (Bubble Chart) mapping Total Shots against Shots on Target, sized by total Goals Scored.
   - Analyzes team discipline and aggressiveness with a Stacked Bar Chart tracking Yellow vs Red Cards per club.

Page 3: Player Stats
   - Narrows the focus specifically to the 2024-2025 season to identify current star players from the dim_players table.
   - Features a 'Top Scores & Assists' Heatmap Table ranking attacking contributions.
   - Analyzes striking efficiency with a Scatter Plot comparing Shots on Target vs Goal Contributions, including a targeted annotation explaining a Data Anomaly regarding scraped goal values for newly promoted teams.

Live Dashboard: [View the Interactive Dashboard Here](https://lookerstudio.google.com/reporting/1934755d-8c3c-4059-8bd9-5364b4e85ca8)

To publish Looker Studio: click Share -> Manage access -> Anyone with the link can view -> copy the link.

---

## How to Reproduce

Follow these steps exactly to run this project from scratch on your own GCP account. Each step builds on the previous one. Do not skip steps.

### Prerequisites

| Requirement | Why It Is Needed | Install Link |
|---|---|---|
| Python 3.11+ | Runs the data ingestion, and also runs dbt. | https://www.python.org/downloads/ |
| Terraform | Provisions the GCP cloud infrastructure | https://developer.hashicorp.com/terraform/install |
| dbt-bigquery | dbt adapter that allows dbt to connect to BigQuery | pip install dbt-bigquery |
| Google Cloud SDK | Required to authenticate with GCP from your local machine | https://cloud.google.com/sdk/docs/install |
| GCP account | BigQuery costs money (very small amounts). Free tier covers this. | https://console.cloud.google.com/ |

GCP Service Account setup:
A service account is a special GCP identity that lets tools like Terraform and dbt access your GCP project programmatically (without you needing to log in interactively).

1. Go to https://console.cloud.google.com/
2. Select your project (or create a new one)
3. Navigate to IAM & Admin -> Service Accounts
4. Click Create Service Account
5. Give it a name (e.g., epl-pipeline-sa)
6. Assign the following roles:
   - BigQuery Admin
   - Storage Admin
7. Click Done, then click the service account you just created
8. Go to the Keys tab -> Add Key -> Create new key -> JSON
9. A .json file will download automatically - keep this file safe

WARNING: Never commit the .json key file to Git. It grants full access to your GCP project. The .gitignore in this repo already excludes *.json files.

---

### Step 1 - Clone the Repository

Open a terminal (PowerShell on Windows, Terminal on macOS/Linux) and run:

```bash
git clone https://github.com/khiemdztv/de-zoomcamp-2026-project-.git
cd "de-zoomcamp-2026-project-"
```

---

### Step 2 - Place Your GCP Service Account Key

Move the .json key file you downloaded from GCP into the root of the project folder and rename it to:

```
epl-data-pipeline-9999-de6a9c38ce47.json
```

This exact filename is referenced in `terraform/main.tf` and python scripts. If you use a different filename, you must update the credentials line in `main.tf` and scripts to match.

---

### Step 3 - Configure Terraform Variables

Open `terraform/variables.tf` to see what variables are required. Create a file called `terraform/terraform.tfvars` with your actual values:

```hcl
project_id = "your-gcp-project-id"
region     = "asia-southeast1"
```

Replace `your-gcp-project-id` with your actual GCP project ID (visible in the GCP console top bar).

---

### Step 4 - Provision Infrastructure with Terraform

```bash
cd terraform

# Download the GCP provider plugin (only needed once)
terraform init

# Preview what Terraform will create - no changes made yet
terraform plan

# Create the BigQuery datasets on GCP
terraform apply
```

When prompted, type yes and press Enter.

After this completes, you will have two BigQuery datasets in your GCP project:
- `epl_raw` - ready to receive raw data
- `epl_core` - ready to receive transformed tables from dbt

Go back to the project root:
```bash
cd ..
```

---

### Step 5 - Load Data using Python Ingestion

Install the required Python packages in your command line:
```bash
pip install pandas pandas-gbq google-cloud-bigquery db-dtypes
```

Before running, open `load_to_bigquery.py` in your code editor. Check the top of the file:
- Ensure `PROJECT_ID` strictly matches your Google Cloud Project ID.
- Ensure `KEY_FILE` path strictly matches where you placed your JSON key.

Run the ingestion script from the project root. This handles reading the local CSVs/JSONs and streaming them into BigQuery objects:
```bash
python load_to_bigquery.py
```

Wait for the script to finish processing the arrays and printing "Successfully Loaded!".

---

### Step 6 - Run dbt Transformations

```bash
# Go directly into the dbt project folder
cd epl_dbt

# Test that dbt can connect to BigQuery
dbt debug

# Run all models (creates staging views + mart tables in BigQuery epl_core dataset)
dbt run

# Run automated data quality tests
dbt test

# Optional: generate HTML documentation and view it in your browser
dbt docs generate
dbt docs serve
```

If `dbt debug` fails, check that your `profiles.yml` (located in your system user profile `~/.dbt/`) is correctly configured with your GCP project ID and the path to your service account key file.

After `dbt run` completes, you will find 8 models (including fct_matches, dim_players) located inside BigQuery under the `epl_core` dataset.

---

### Step 7 - Build the Dashboard in Looker Studio

1. Go to Looker Studio.
2. Click Create -> Report
3. Select BigQuery as the data source connector
4. Choose your GCP project -> `epl_core` dataset -> select `fct_matches`
5. Click Add to add the data source
6. Build your charts (bar charts, tables, line charts) following the Dashboard strategy specified previously.
7. To add `dim_players` as a second data source, click Resource -> Manage added data sources -> Add a data source.
8. When done, click Share -> Manage access -> set to Anyone with the link can view -> copy the link
9. Paste the link into the Dashboard section at the top of this README

---

### Step 8 - Tear Down (Optional)

When you are done and want to stop paying for GCP resources, simply drop the BigQuery datasets using Terraform:

```bash
cd terraform
terraform destroy
```

Type yes when prompted. This will exclusively delete the BigQuery datasets and all tables inside them.

---

## Lessons Learned

- Cloud billing restrictions force creative solutions. We were originally blocked because Google Cloud returned 403 Forbidden errors when Kestra attempted to create a Cloud Storage Bucket via Terraform. By bypassing GCS entirely and utilizing purely Python to interface with BigQuery APIs directly, we preserved the ELT functionality without cloud service halts.
- Infrastructure as Code pays off immediately. Using Terraform meant the entire GCP setup could be torn down and recreated in minutes. No clicking around in the console, and no risk of forgetting which settings were used.
- Two datasets (epl_raw and epl_core) is better than one. Keeping raw data isolated from transformed data means you can always re-run dbt from scratch without scraping or reloading anything. The raw data remains the ultimate source of truth.
- Partition complications taught us to read Cloud documentation carefully. BigQuery dataset expiration policies directly affect partition dates. We learned to recreate non-partitioned tables when data timestamps fall outside the strict 60-day expiration window.
- Separating staging from marts is not over-engineering. When raw data format changes across seasons, only the staging models needed updating - the mart logic stayed the same. This modularity saved significant engineering time.
- Never commit credentials to Git. The `.gitignore` file must exclude `*.json` key files before the very first commit.
