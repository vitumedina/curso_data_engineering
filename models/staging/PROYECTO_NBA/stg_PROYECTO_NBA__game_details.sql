with 

source as (

    select * from {{ source('PROYECTO_NBA', 'game_details') }}

),

renamed as (

    select
        game_id::NUMERIC(25,0) AS game_id,
        team_id::NUMERIC(25,0) AS team_id,
        team_abbreviation::VARCHAR(50) AS team_abbreviation,
        team_city::VARCHAR(50) AS team_city,
        player_id::NUMERIC(25,0) AS player_id,
        player_name::VARCHAR(50) AS player_name,
        nickname::VARCHAR(50) AS nickname,
        IFNULL(start_position, 'No titular')::VARCHAR(50) AS start_position,
        comment IS NULL AS player_participated,
        IFNULL(minutes::VARCHAR(50),'El jugador no jugó en el partido') AS minutes_varchar,
        IFNULL(SPLIT_PART(minutes, ':', 1), 0)::NUMERIC(10,0) AS rounded_mins_played,
        IFNULL(fgm::NUMERIC(10,0), 0) AS fieldgoal_made,
        IFNULL(fga::NUMERIC(10,0), 0) AS fieldgoal_attempted,
        IFNULL(fg_pct::NUMERIC(10,3), 0) AS fieldgoal_pct,
        IFNULL(fg3m::NUMERIC(10,0), 0) AS fieldgoal3_made,
        IFNULL(fg3a::NUMERIC(10,0), 0) AS fieldgoal3_attempted,
        IFNULL(fg3_pct::NUMERIC(10,3), 0) AS fieldgoal3_pct,
        IFNULL(ftm::NUMERIC(10,0), 0) AS freethrows_made,
        IFNULL(fta::NUMERIC(10,0), 0) AS freethrows_attempted,
        IFNULL(ft_pct::NUMERIC(10,3), 0) AS freethrows_pct,
        IFNULL(oreb::NUMERIC(10,0), 0) AS oreb,
        IFNULL(dreb::NUMERIC(10,0), 0) AS dreb,
        IFNULL(reb::NUMERIC(10,0), 0) AS rebounds,
        IFNULL(ast::NUMERIC(10,0), 0) AS assists,
        IFNULL(stl::NUMERIC(10,0), 0) AS steals,
        IFNULL(blk::NUMERIC(10,0), 0) AS blocks,
        IFNULL(turnovers::NUMERIC(10,0), 0) AS turnovers,
        IFNULL(pf::NUMERIC(10,0), 0) AS personal_fouls,
        IFNULL(pts::NUMERIC(10,0), 0) AS pts,
        IFNULL(plus_minus::NUMERIC(10,0), 0) AS plus_minus

    from source

)

select * from renamed
