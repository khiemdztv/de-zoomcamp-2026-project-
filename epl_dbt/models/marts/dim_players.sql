-- dim_players.sql
-- Mart model: player dimension table for 2024/25 EPL season
-- One row per player with all stats, enriched with derived metrics

{{ config(materialized='table') }}

SELECT
    player_name,
    nationality,
    club,
    position,
    season,
    total_appearances,
    sub_appearances,
    minutes_played,
    goals,
    assists,
    goals + assists                                          AS goal_contributions,
    SAFE_DIVIDE(goals, NULLIF(total_appearances, 0))        AS goals_per_game,
    SAFE_DIVIDE(assists, NULLIF(total_appearances, 0))      AS assists_per_game,
    yellow_cards,
    red_cards,
    ROUND(xg, 2)                                            AS xg,
    ROUND(xa, 2)                                            AS xa,
    ROUND(xg + xa, 2)                                       AS xgi,
    shots_on_target,
    pass_attempts,
    ROUND(pass_accuracy_pct, 1)                             AS pass_accuracy_pct,
    duels_won,
    total_tackles,
    interceptions,
    fouls,
    dribble_attempts,
    ROUND(dribble_accuracy_pct, 1)                          AS dribble_accuracy_pct,
    touches_in_opp_box,
    aerial_duels_won,
    -- Goalkeeper-specific
    clean_sheets,
    saves_made,
    goals_conceded,
    penalties_scored,
    -- Data quality flag
    CASE WHEN total_appearances = 0 THEN TRUE ELSE FALSE END AS is_unused_squad_player

FROM {{ ref('stg_players') }}

ORDER BY goals DESC, assists DESC
