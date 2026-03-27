from google.cloud import bigquery
from google.oauth2 import service_account

PROJECT_ID = 'epl-data-pipeline-9999'
KEY_FILE   = r'D:\Documents\DE Zoomcamp 2026 Final Project\epl-data-pipeline-9999-de6a9c38ce47.json'
credentials = service_account.Credentials.from_service_account_file(KEY_FILE)
client = bigquery.Client(project=PROJECT_ID, credentials=credentials)

query = """
    create or replace table `epl-data-pipeline-9999.epl_core.fct_matches`
    as (
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
    );
"""
try:
    q = client.query(query)
    q.result()
    q2 = client.query('SELECT count(*) as c FROM `epl-data-pipeline-9999.epl_core.fct_matches`')
    print('Count after manual create WITHOUT partition:', list(q2)[0].get('c'))
except Exception as e:
    print('Error:', e)
