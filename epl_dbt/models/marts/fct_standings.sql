-- fct_standings.sql
-- Mart model: final league table standings with rank and derived metrics
-- One row per team per season across 9 EPL seasons

{{ config(materialized='table') }}

SELECT
    team_name,
    final_position,
    games_played,
    wins,
    draws,
    losses,
    goals_for,
    goals_against,
    goal_difference,
    points,

    -- Derived metrics
    ROUND(SAFE_DIVIDE(wins, NULLIF(games_played, 0)) * 100, 1)          AS win_pct,
    ROUND(SAFE_DIVIDE(goals_for, NULLIF(games_played, 0)), 2)           AS goals_per_game,
    ROUND(SAFE_DIVIDE(goals_against, NULLIF(games_played, 0)), 2)       AS goals_conceded_per_game,

    -- Tier classification
    CASE
        WHEN final_position <= 4  THEN 'Champions League'
        WHEN final_position = 5   THEN 'Europa League'
        WHEN final_position = 6   THEN 'Conference League'
        WHEN final_position >= 18 THEN 'Relegated'
        ELSE 'Mid-Table'
    END                                                                  AS season_zone

FROM {{ ref('stg_standings') }}

ORDER BY final_position
