WITH stg_games AS (
    SELECT * 
    FROM {{ ref('stg_PROYECTO_NBA__games') }}
    ),

stg_teams AS (
    SELECT * 
    FROM {{ ref('stg_PROYECTO_NBA__teams') }}
    ),

renamed_casted AS (
    SELECT g.*,
        h.*,
        a.headcoach AS visitor_coach
    FROM stg_games g 
    INNER JOIN stg_teams h ON h.team_id = g.team_id_home
    INNER JOIN stg_teams a ON a.team_id = g.team_id_away
    )

SELECT * FROM renamed_casted