-- stg_club_stats.sql
-- Staging model: clean and standardize club statistics from epl_raw.raw_club_stats
-- Source: 9 seasons of club stats CSV files loaded into BigQuery

{{ config(materialized='view') }}

SELECT
    TRIM(club_name)                                         AS club_name,
    TRIM(season)                                            AS season,
    SAFE_CAST(`Games Played`       AS INT64)                AS games_played,
    SAFE_CAST(Goals                AS INT64)                AS goals_scored,
    SAFE_CAST(`Goals Conceded`     AS INT64)                AS goals_conceded,
    SAFE_CAST(XG                   AS FLOAT64)              AS xg,
    SAFE_CAST(Shots                AS INT64)                AS shots,
    SAFE_CAST(`Shots On Target`    AS INT64)                AS shots_on_target,
    SAFE_CAST(Passes               AS INT64)                AS passes,
    SAFE_CAST(long_passes          AS INT64)                AS long_passes,
    SAFE_CAST(long_pass_accuracy   AS FLOAT64)              AS long_pass_accuracy_pct,
    SAFE_CAST(`Corners Taken`      AS INT64)                AS corners_taken,
    SAFE_CAST(dribble_attempts     AS INT64)                AS dribble_attempts,
    SAFE_CAST(dribble_accuracy     AS FLOAT64)              AS dribble_accuracy_pct,
    SAFE_CAST(Interceptions        AS INT64)                AS interceptions,
    SAFE_CAST(Blocks               AS INT64)                AS blocks,
    SAFE_CAST(Clearances           AS INT64)                AS clearances,
    SAFE_CAST(`Yellow Cards`       AS INT64)                AS yellow_cards,
    SAFE_CAST(`Red Cards`          AS INT64)                AS red_cards,
    SAFE_CAST(Fouls                AS INT64)                AS fouls,
    SAFE_CAST(penalties            AS INT64)                AS penalties_awarded,
    SAFE_CAST(penalties_scored     AS INT64)                AS penalties_scored

FROM {{ source('epl_raw', 'raw_club_stats') }}
WHERE club_name IS NOT NULL
  AND season IS NOT NULL
