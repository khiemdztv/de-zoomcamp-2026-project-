-- stg_matches.sql
-- Staging model: clean and extract match-level data from epl_raw.raw_matches_2425
-- Source: matches_2425.json — flat array of 380 match records for 2024/25 season
-- Fields: date, time, day, home, away, goals_home, goals_away, attendance, venue, referee

{{ config(materialized='view') }}

SELECT
    -- Synthetic surrogate key based on ordered date + teams
    ROW_NUMBER() OVER (ORDER BY date, home, away)               AS match_id,
    TRIM(home)                                                  AS home_team_name,
    TRIM(away)                                                  AS away_team_name,
    SAFE_CAST(goals_home    AS INT64)                           AS home_goals,
    SAFE_CAST(goals_away    AS INT64)                           AS away_goals,

    -- Derived: match result from home team perspective
    CASE
        WHEN SAFE_CAST(goals_home AS INT64) > SAFE_CAST(goals_away AS INT64) THEN 'H'
        WHEN SAFE_CAST(goals_home AS INT64) < SAFE_CAST(goals_away AS INT64) THEN 'A'
        WHEN SAFE_CAST(goals_home AS INT64) = SAFE_CAST(goals_away AS INT64) THEN 'D'
        ELSE NULL
    END                                                         AS result,

    SAFE_CAST(goals_home AS INT64)
        + SAFE_CAST(goals_away AS INT64)                        AS total_goals,

    SAFE.PARSE_DATE('%Y-%m-%d', date)                           AS match_date,
    CAST(time AS STRING)                                        AS kick_off_time,
    TRIM(venue)                                                 AS stadium,
    TRIM(referee)                                               AS referee,

    -- Clean attendance: remove commas and cast to integer
    SAFE_CAST(REGEXP_REPLACE(CAST(attendance AS STRING), r'[^0-9]', '') AS INT64)
                                                                AS attendance,
    '2024/2025'                                                 AS season

FROM {{ source('epl_raw', 'raw_matches_2425') }}

WHERE home IS NOT NULL
  AND away IS NOT NULL
  AND goals_home IS NOT NULL
  AND goals_away IS NOT NULL
