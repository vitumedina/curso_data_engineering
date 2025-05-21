{{
    config(
        materialized = "table"
    )
}}

with team_players as (
    SELECT
        p.player_id,
        p.player_name,
        p.team_id,
        p.abbreviation,
        p.season AS player_team_season, -- Temporada de la tabla int_player_teams
        s.season_desc as season,        -- Temporada de la tabla int_accumulated_player_stats, renombrada como 'season'
        s.total_minutes_played,
        s.total_fieldgoal_made,
        s.total_fieldgoal_attempted,
        s.fieldgoal_pct,
        s.total_freethrows_attempted,
        s.total_freethrows_made,
        s.freethrows_pct,
        s.total_fieldgoal3_attempted,
        s.total_fieldgoal3_made,
        s.fieldgoal3_pct,
        s.total_oreb,
        s.total_dreb,
        s.total_rebounds,
        s.total_assists,
        s.total_steals,
        s.total_blocks,
        s.total_turnovers,
        s.total_personal_fouls,
        s.total_pts,
        s.total_plus_minus
    FROM {{ ref('int_player_teams') }} p
    INNER JOIN {{ ref('int_accumulated_player_stats') }} s
        ON p.player_id = s.player_id
        AND p.season = s.season_desc -- Asegúrate que 'season' en int_player_teams y 'season_desc' en int_accumulated_player_stats son del mismo tipo y coinciden.
)

, player_career as (
    SELECT
        season, -- <--- AHORA SÍ: SELECCIONA LA COLUMNA REAL 'season'
        player_id,
        player_name,
        team_id,
        abbreviation,
        -- Todas las métricas que vienen de team_players
        ROW_NUMBER() OVER (PARTITION BY player_id ORDER BY season ASC) AS season_in_nba,
        ROW_NUMBER() OVER (PARTITION BY player_id, team_id ORDER BY season ASC) AS year_in_team,
        CASE WHEN ROW_NUMBER() OVER (PARTITION BY player_id, team_id ORDER BY season ASC) = 1 THEN TRUE
             ELSE FALSE END AS new_signing,
        total_minutes_played,
        total_fieldgoal_made,
        total_fieldgoal_attempted,
        fieldgoal_pct,
        total_freethrows_attempted,
        total_freethrows_made,
        freethrows_pct,
        total_fieldgoal3_attempted,
        total_fieldgoal3_made,
        fieldgoal3_pct,
        total_oreb,
        total_dreb,
        total_rebounds,
        total_assists,
        total_steals,
        total_blocks,
        total_turnovers,
        total_personal_fouls,
        total_pts,
        total_plus_minus

    FROM team_players
)

SELECT * from player_career
-- Ahora estos WHERE/ORDER BY funcionarán porque las columnas existen en player_career
-- where season = 2019 and abbreviation = 'LAL'
-- where player_name = 'Alex Caruso'
ORDER BY player_name, season_in_nba ASC