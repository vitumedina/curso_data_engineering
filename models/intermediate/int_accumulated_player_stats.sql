{{
    config(
        materialized = "table"
    )
}}

-- CTE para unir los detalles del juego con la información general del juego
-- para obtener la fecha y la temporada.
with game_details_with_game_info as (
    SELECT
        gd.*, -- Selecciona todas las columnas de game_details
        g.game_date_est, -- Trae la fecha del partido
        g.season_desc         -- Trae la temporada del partido
    FROM {{ ref('stg_PROYECTO_NBA__game_details') }} gd
    INNER JOIN {{ ref('stg_PROYECTO_NBA__games') }} g -- Unir con la tabla de partidos
        ON gd.game_id = g.game_id -- La unión se hace por GAME_ID
)

-- CTE para calcular las estadísticas agregadas por jugador y temporada
, player_season_stats as (
    SELECT
        player_id,
        season_desc, -- Incluye la temporada en la agrupación
        MAX(player_name) AS player_name, -- MAX(player_name) para mantener el nombre
        SUM(rounded_mins_played) AS total_minutes_played,
        SUM(FIELDGOAL_MADE) AS total_fieldgoal_made,
        SUM(FIELDGOAL_ATTEMPTED) AS total_fieldgoal_attempted,
        DIV0(SUM(FIELDGOAL_MADE),SUM(FIELDGOAL_ATTEMPTED)) AS fieldgoal_pct,
        SUM(FREETHROWS_ATTEMPTED) AS total_freethrows_attempted,
        SUM(freethrows_made) AS total_freethrows_made,
        DIV0(SUM(freethrows_made),SUM(FREETHROWS_ATTEMPTED)) AS freethrows_pct,
        SUM(fieldgoal3_attempted) AS total_fieldgoal3_attempted,
        SUM(fieldgoal3_made) AS total_fieldgoal3_made,
        DIV0(SUM(fieldgoal3_made),SUM(fieldgoal3_attempted)) AS fieldgoal3_pct, -- Cambiado el alias para evitar duplicidad con el campo
        SUM(OREB) AS total_oreb,
        SUM(DREB) AS total_dreb,
        SUM(REBOUNDS) AS total_rebounds,
        SUM(ASSISTS) AS total_assists,
        SUM(STEALS) AS total_steals,
        SUM(BLOCKS) AS total_blocks,
        SUM(TURNOVERS) AS total_turnovers,
        SUM(PERSONAL_FOULS) AS total_personal_fouls,
        SUM(PTS) AS total_pts,
        SUM(PLUS_MINUS) AS total_plus_minus
    FROM game_details_with_game_info 
    GROUP BY
        player_id,
        season_desc 
)

SELECT *
FROM player_season_stats
ORDER BY player_name, season_desc 