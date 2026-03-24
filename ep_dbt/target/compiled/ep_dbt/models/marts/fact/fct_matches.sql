

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
from `epl-data-pipeline-9999`.`epl_core`.`stg_matches`