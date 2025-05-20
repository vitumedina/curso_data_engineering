WITH stg_game_player AS (
    SELECT * 
    FROM {{ ref('stg_PROYECTO_NBA__players') }}
    ),

stg_teams AS (
    SELECT * 
    FROM {{ ref('stg_PROYECTO_NBA__teams') }}
    ),

renamed_casted AS (
    SELECT player_name
    , player_id
    , season
    , t.team_id
    , abbreviation
    
    FROM stg_game_player p 
    INNER JOIN stg_teams t ON t.team_id = p.team_id
    )

SELECT * FROM renamed_casted