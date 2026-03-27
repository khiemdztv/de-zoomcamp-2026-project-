-- stg_players.sql
-- Staging model: clean and standardize player statistics from epl_raw.raw_player_stats
-- Source: 2024/2025 season player stats + player info joined together

{{ config(materialized='view') }}

WITH player_stats AS (
    SELECT
        TRIM(player_name)                                       AS player_name,
        SAFE_CAST(appearances_          AS INT64)               AS total_appearances,
        SAFE_CAST(sub_appearances       AS INT64)               AS sub_appearances,
        SAFE_CAST(`Minutes Played`      AS INT64)               AS minutes_played,
        SAFE_CAST(Goals                 AS INT64)               AS goals,
        SAFE_CAST(Assists               AS INT64)               AS assists,
        SAFE_CAST(`Yellow Cards`        AS INT64)               AS yellow_cards,
        SAFE_CAST(`Red Cards`           AS INT64)               AS red_cards,
        SAFE_CAST(XG                    AS FLOAT64)             AS xg,
        SAFE_CAST(XA                    AS FLOAT64)             AS xa,
        SAFE_CAST(`Shots On Target Inside the Box` AS INT64)    AS shots_on_target,
        SAFE_CAST(pass_attempts         AS INT64)               AS pass_attempts,
        SAFE_CAST(pass_accuracy         AS FLOAT64)             AS pass_accuracy_pct,
        SAFE_CAST(`Duels Won`           AS INT64)               AS duels_won,
        SAFE_CAST(`Total Tackles`       AS INT64)               AS total_tackles,
        SAFE_CAST(Interceptions         AS INT64)               AS interceptions,
        SAFE_CAST(Fouls                 AS INT64)               AS fouls,
        SAFE_CAST(dribble_attempts      AS INT64)               AS dribble_attempts,
        SAFE_CAST(dribble_accuracy      AS FLOAT64)             AS dribble_accuracy_pct,
        SAFE_CAST(`Touches in the Opposition Box` AS INT64)     AS touches_in_opp_box,
        SAFE_CAST(`Aerial Duels Won`    AS INT64)               AS aerial_duels_won,
        SAFE_CAST(`Clean Sheets`        AS INT64)               AS clean_sheets,
        SAFE_CAST(`Saves Made`          AS INT64)               AS saves_made,
        SAFE_CAST(`Goals Conceded`      AS INT64)               AS goals_conceded,
        SAFE_CAST(penalties_scored      AS INT64)               AS penalties_scored,
        '2024/2025'                                             AS season
    FROM {{ source('epl_raw', 'raw_player_stats') }}
    WHERE player_name IS NOT NULL
),

player_info AS (
    SELECT
        TRIM(player_name)               AS player_name,
        TRIM(player_country)            AS nationality,
        TRIM(player_club)               AS club,
        TRIM(player_position)           AS position
    FROM {{ source('epl_raw', 'raw_player_info') }}
    WHERE player_name IS NOT NULL
)

SELECT
    ps.player_name,
    pi.nationality,
    pi.club,
    pi.position,
    ps.season,
    ps.total_appearances,
    ps.sub_appearances,
    ps.minutes_played,
    ps.goals,
    ps.assists,
    ps.yellow_cards,
    ps.red_cards,
    ps.xg,
    ps.xa,
    ps.shots_on_target,
    ps.pass_attempts,
    ps.pass_accuracy_pct,
    ps.duels_won,
    ps.total_tackles,
    ps.interceptions,
    ps.fouls,
    ps.dribble_attempts,
    ps.dribble_accuracy_pct,
    ps.touches_in_opp_box,
    ps.aerial_duels_won,
    ps.clean_sheets,
    ps.saves_made,
    ps.goals_conceded,
    ps.penalties_scored

FROM player_stats ps
LEFT JOIN player_info pi ON ps.player_name = pi.player_name
