-- fct_matches.sql
-- Mart model: final match facts table for 2024/25 EPL season
-- Based on stg_matches which uses the actual matches_2425.json schema
-- Materialized as a partitioned TABLE for fast dashboard queries

{{ config(
    materialized='table',
    cluster_by=["home_team_name", "away_team_name"]
) }}

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



FROM {{ ref('stg_matches') }}
