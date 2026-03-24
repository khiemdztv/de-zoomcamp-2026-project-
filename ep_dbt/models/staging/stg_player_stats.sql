{{ config(materialized='view') }}

select * from `epl-data-pipeline-9999.epl_raw.player_stats_2425`