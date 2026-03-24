

  create or replace view `epl-data-pipeline-9999`.`epl_core`.`stg_player_stats`
  OPTIONS()
  as 

select * from `epl-data-pipeline-9999.epl_raw.player_stats_2425`;

