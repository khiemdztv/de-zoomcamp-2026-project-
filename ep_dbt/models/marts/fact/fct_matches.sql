{{ config(materialized='table') }}

select
    match_id,
    home_team,
    away_team,
    goals_home,
    goals_away,
    date,
    time,
    venue,
    referee,
    result
from {{ ref('stg_matches') }}