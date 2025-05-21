with 

source as (

    select * from {{ source('PROYECTO_NBA', 'ranking') }}

),

renamed as (

    select
        team_id::NUMERIC(25,0) AS team_id,
        league_id::NUMERIC(25,0) AS league_id,
        RIGHT(season_id,4)::NUMERIC(25,0) AS season,
        TO_DATE(standingsdate) AS standingsdate,
        conference::VARCHAR(50) AS conference,
        team::VARCHAR(50) AS team,
        g::NUMERIC(25,0) AS games_played,
        w::NUMERIC(25,0) AS win_count,
        l::NUMERIC(25,0) AS lose_count,
        w_pct::NUMERIC(25,3) AS win_pct,
        (SPLIT_PART(home_record, '-',1) + SPLIT_PART(home_record, '-',2)):: NUMERIC(25,0) AS home_games,
        home_record::VARCHAR(50) AS home_record,
        DIV0(SPLIT_PART(home_record, '-',1),SPLIT_PART(home_record, '-',1) + SPLIT_PART(home_record, '-',2)):: NUMERIC(25,3) AS home_win_pct,
        road_record::VARCHAR(50) AS road_record,
        (SPLIT_PART(road_record, '-',1) + SPLIT_PART(road_record, '-',2)):: NUMERIC(25,0) AS away_games,
        DIV0(SPLIT_PART(road_record, '-',1),SPLIT_PART(road_record, '-',1) + SPLIT_PART(road_record, '-',2)):: NUMERIC(25,3) AS away_win_pct,
        IFNULL(returntoplay, 'Not Covid')::VARCHAR(50) AS returntoplay

    from source

)

select * from renamed
-- WHERE SEASON != YEAR(standingsdate) AND SEASON + 1 != YEAR(standingsdate)
