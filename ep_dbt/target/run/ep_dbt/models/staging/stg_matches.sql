

  create or replace view `epl-data-pipeline-9999`.`epl_core`.`stg_matches`
  OPTIONS()
  as 

select * from `epl-data-pipeline-9999.epl_raw.fact_matches`;

