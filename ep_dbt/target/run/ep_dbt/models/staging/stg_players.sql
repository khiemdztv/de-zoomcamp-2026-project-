

  create or replace view `epl-data-pipeline-9999`.`epl_core`.`stg_players`
  OPTIONS()
  as 

select * 
from `epl-data-pipeline-9999.epl_raw.player_info`;

