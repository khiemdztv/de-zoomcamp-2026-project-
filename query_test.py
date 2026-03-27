from google.cloud import bigquery
from google.oauth2 import service_account

PROJECT_ID = 'epl-data-pipeline-9999'
KEY_FILE   = r'D:\Documents\DE Zoomcamp 2026 Final Project\epl-data-pipeline-9999-de6a9c38ce47.json'
credentials = service_account.Credentials.from_service_account_file(KEY_FILE)
client = bigquery.Client(project=PROJECT_ID, credentials=credentials)

query = """
SELECT
    match_id,
    home_team_name,
    away_team_name,
    home_goals,
    away_goals,
    total_goals,
    result,
    match_date,
    EXTRACT(YEAR  FROM match_date)                          AS match_year,
    EXTRACT(MONTH FROM match_date)                         AS match_month,
    EXTRACT(WEEK  FROM match_date)                         AS match_week_of_year,
    kick_off_time,
    stadium,
    referee,
    attendance,
    season
FROM `epl-data-pipeline-9999.epl_core.stg_matches`
"""
q = client.query(query)
rows = list(q)
print(f'Rows returned: {len(rows)}')
if len(rows) > 0:
    print(dict(rows[0]))
