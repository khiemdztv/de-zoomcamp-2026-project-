"""
load_to_bigquery.py
Direct ingestion script: loads all EPL datasets straight into BigQuery epl_raw dataset.
Use this when GCS is unavailable (e.g., billing disabled).

Requirements: pip install google-cloud-bigquery pandas pyarrow
"""

import os
import glob
import json
import pandas as pd
from google.cloud import bigquery
from google.oauth2 import service_account

# ─── Config ──────────────────────────────────────────────────────────────────
PROJECT_ID   = "epl-data-pipeline-9999"
DATASET_ID   = "epl_raw"
LOCATION     = "asia-southeast1"
KEY_FILE     = r"D:\Documents\DE Zoomcamp 2026 Final Project\epl-data-pipeline-9999-de6a9c38ce47.json"
BASE_DIR     = r"D:\Documents\DE Zoomcamp 2026 Final Project\datasets 9 seasons"
MATCHES_FILE = r"D:\Documents\DE Zoomcamp 2026 Final Project\matches_2425.json"

SEASONS = [2016, 2017, 2018, 2019, 2020, 2021, 2022, 2023, 2024]

# ─── Client ──────────────────────────────────────────────────────────────────
credentials = service_account.Credentials.from_service_account_file(KEY_FILE)
client = bigquery.Client(project=PROJECT_ID, credentials=credentials, location=LOCATION)

def load_df_to_bq(df: pd.DataFrame, table_name: str, write_disposition="WRITE_TRUNCATE"):
    """Load a pandas DataFrame into BigQuery."""
    table_id = f"{PROJECT_ID}.{DATASET_ID}.{table_name}"
    job_config = bigquery.LoadJobConfig(
        write_disposition=write_disposition,
        autodetect=True,
    )
    job = client.load_table_from_dataframe(df, table_id, job_config=job_config)
    job.result()
    table = client.get_table(table_id)
    print(f"  [OK] {table_name}: {table.num_rows:,} rows loaded")


# ─── 1. Club Stats (9 seasons) ────────────────────────────────────────────────
print("\n[1/4] Loading club stats...")
club_dfs = []
for season in SEASONS:
    path = os.path.join(BASE_DIR, "club_stats", f"{season}_season_club_stats.csv")
    if os.path.exists(path):
        df = pd.read_csv(path)
        df["season"] = f"{season}/{season+1}"
        club_dfs.append(df)
        print(f"  Read {len(df)} rows from {season} season")
    else:
        print(f"  WARNING: {path} not found")

if club_dfs:
    all_club = pd.concat(club_dfs, ignore_index=True)
    load_df_to_bq(all_club, "raw_club_stats")


# ─── 2. Player Stats (2024/25) ────────────────────────────────────────────────
print("\n[2/4] Loading player stats...")
player_stats_path = os.path.join(BASE_DIR, "player_stats_2024_2025_season.csv")
player_info_path  = os.path.join(BASE_DIR, "premier_player_info.csv")

if os.path.exists(player_stats_path):
    df = pd.read_csv(player_stats_path)
    load_df_to_bq(df, "raw_player_stats")
else:
    print(f"  WARNING: {player_stats_path} not found")

if os.path.exists(player_info_path):
    df = pd.read_csv(player_info_path)
    load_df_to_bq(df, "raw_player_info")
else:
    print(f"  WARNING: {player_info_path} not found")


# ─── 3. League Table Final Standings (GW38 per season) ───────────────────────
print("\n[3/4] Loading league table standings...")
standings_dfs = []
for season in SEASONS:
    folder = os.path.join(BASE_DIR, "league_table", "home_and_away", f"gameweek_{season}")
    # Use final gameweek (38)
    path = os.path.join(folder, f"{season}_gameweek_38.csv")
    if os.path.exists(path):
        df = pd.read_csv(path)
        df["season"] = f"{season}/{season+1}"
        standings_dfs.append(df)
        print(f"  Read {len(df)} rows from {season} GW38")
    else:
        print(f"  WARNING: {path} not found")

if standings_dfs:
    all_standings = pd.concat(standings_dfs, ignore_index=True)
    load_df_to_bq(all_standings, "raw_standings")


# ─── 4. Matches 2024/25 (JSON) ───────────────────────────────────────────────
print("\n[4/4] Loading matches 2024/25...")
if os.path.exists(MATCHES_FILE):
    with open(MATCHES_FILE, "r", encoding="utf-8") as f:
        matches_data = json.load(f)
    # Flatten JSON array — each element is a match
    if isinstance(matches_data, list):
        rows = matches_data
    elif isinstance(matches_data, dict):
        # Handle nested structure if needed
        rows = list(matches_data.values())[0] if len(matches_data) == 1 else [matches_data]
    
    df = pd.json_normalize(rows)
    # Drop nested 'details' column which has complex structure
    df = df.drop(columns=[c for c in df.columns if c.startswith("details")], errors="ignore")
    load_df_to_bq(df, "raw_matches_2425")
else:
    print(f"  WARNING: {MATCHES_FILE} not found")

print("\n✅ All data loaded into BigQuery epl_raw! Run dbt next.")
