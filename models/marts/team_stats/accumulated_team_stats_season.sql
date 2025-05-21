{{
    config(
        materialized = "table"
    )
}}

with games_stats as (
        SELECT * 
        FROM {{ ref('stg_PROYECTO_NBA__games') }} -- d 

)

, team_stats as (
    SELECT *
    -- player_id
    -- , MAX(player_name) AS player_name
    -- , SUM(rounded_mins_played) AS rounded_mins_played
    -- , SUM(FIELDGOAL_MADE)  AS FIELDGOAL_MADE
    -- , SUM(FIELDGOAL_ATTEMPTED) AS FIELDGOAL_ATTEMPTED
    -- , DIV0(SUM(FIELDGOAL_MADE),SUM(FIELDGOAL_ATTEMPTED)) AS FIELDGOAL_PCT
    -- , SUM(FREETHROWS_ATTEMPTED) AS FREETHROWS_ATTEMPTED
    -- , SUM(freethrows_made) AS freethrows_made
    -- , DIV0(SUM(freethrows_made),SUM(FREETHROWS_ATTEMPTED)) AS freethrows_pct
    -- , SUM(fieldgoal3_attempted) AS fieldgoal3_attempted
    -- , SUM(fieldgoal3_made) AS fieldgoal3_made
    -- , DIV0(SUM(fieldgoal3_made),SUM(fieldgoal3_attempted)) AS fieldgoal3_made
    -- , SUM(OREB) AS OREB
    -- , SUM(DREB) AS DREB
    -- , SUM(REBOUNDS) AS REBOUNDS
    -- , SUM(ASSISTS) AS ASSISTS
    -- , SUM(STEALS) AS STEALS
    -- , SUM(BLOCKS) AS BLOCKS
    -- , SUM(TURNOVERS) AS TURNOVERS
    -- , SUM(PERSONAL_FOULS) AS PERSONAL_FOULS
    -- , SUM(PTS) AS PTS
    -- , SUM(PLUS_MINUS) AS PLUS_MINUS
FROM games_stats
-- GROUP BY player_id

)

SELECT * from team_stats
