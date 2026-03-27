-- stg_standings.sql
-- Staging model: clean and standardize league table standings from epl_raw.raw_standings
-- Source: Final gameweek (GW38) standings per season, 9 seasons combined

{{ config(materialized='view') }}

SELECT
    TRIM(name)                                  AS team_name,
    SAFE_CAST(position         AS INT64)        AS final_position,
    SAFE_CAST(games_played     AS INT64)        AS games_played,
    SAFE_CAST(games_won        AS INT64)        AS wins,
    SAFE_CAST(games_drawn      AS INT64)        AS draws,
    SAFE_CAST(games_lost       AS INT64)        AS losses,
    SAFE_CAST(goals_for        AS INT64)        AS goals_for,
    SAFE_CAST(goals_against    AS INT64)        AS goals_against,
    SAFE_CAST(goal_difference  AS INT64)        AS goal_difference,
    SAFE_CAST(points           AS INT64)        AS points

FROM {{ source('epl_raw', 'raw_standings') }}

WHERE name IS NOT NULL
  AND points IS NOT NULL
