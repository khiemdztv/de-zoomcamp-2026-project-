
  
    

    create or replace table `epl-data-pipeline-9999`.`epl_core`.`dim_players`
      
    
    

    
    OPTIONS()
    as (
      

select
    player_name,
    player_country,
    player_club,
    player_position,
    player_image_url
from `epl-data-pipeline-9999`.`epl_core`.`stg_players`
    );
  