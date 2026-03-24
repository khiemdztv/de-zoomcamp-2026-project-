{{ config(materialized='table') }}

select
    player_name,
    player_country,
    player_club,
    player_position,
    player_image_url
from {{ ref('stg_players') }}