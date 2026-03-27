-- dim_teams.sql
-- Mart model: team dimension with key performance indicators per season
-- Source: 9 seasons of club stats data

{{ config(materialized='table') }}

SELECT
    club_name,
    season,
    games_played,
    goals_scored,
    goals_conceded,
    goals_scored - goals_conceded                                           AS goal_difference,
    ROUND(SAFE_DIVIDE(goals_scored, NULLIF(games_played, 0)), 2)           AS goals_scored_per_game,
    ROUND(SAFE_DIVIDE(goals_conceded, NULLIF(games_played, 0)), 2)         AS goals_conceded_per_game,
    ROUND(xg, 2)                                                           AS xg,
    shots,
    shots_on_target,
    ROUND(SAFE_DIVIDE(shots_on_target, NULLIF(shots, 0)) * 100, 1)        AS shot_accuracy_pct,
    passes,
    long_passes,
    ROUND(long_pass_accuracy_pct, 1)                                       AS long_pass_accuracy_pct,
    corners_taken,
    dribble_attempts,
    ROUND(dribble_accuracy_pct, 1)                                         AS dribble_accuracy_pct,
    interceptions,
    blocks,
    clearances,
    yellow_cards,
    red_cards,
    fouls,
    penalties_awarded,
    penalties_scored,
    ROUND(SAFE_DIVIDE(penalties_scored, NULLIF(penalties_awarded, 0)) * 100, 1) AS penalty_conversion_pct

FROM {{ ref('stg_club_stats') }}

ORDER BY season, goals_scored DESC
