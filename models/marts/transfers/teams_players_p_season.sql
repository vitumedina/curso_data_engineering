{{
    config(
        materialized = "table"
    )
}}

with team_players as (
        SELECT *
        FROM {{ ref('int_player_teams') }} -- e
        -- INNER JOIN {{ ref('dim_tiempo') }} t
)

, player_career as (
    SELECT
        season
        -- , season_id 
        , player_name
        , player_id
        , team_id
        , abbreviation
        , ROW_NUMBER() OVER (PARTITION BY player_id ORDER BY season ASC) AS season_in_nba
        -- , ROW_NUMBER() OVER (PARTITION BY team_id ORDER BY season ASC) AS seasons_in_nba
        , ROW_NUMBER() OVER (PARTITION BY player_id, team_id ORDER BY season ASC) AS year_in_team
        , case when year_in_team = 1 then True
            else False end as new_signing
  FROM team_players

)

SELECT * from player_career
-- where season = 2019 and abbreviation = 'LAL' 
-- where player_name = 'Alex Caruso'
ORDER BY season_in_nba ASC