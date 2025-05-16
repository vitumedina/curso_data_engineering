with 

source as (

    select * from {{ source('PROYECTO_NBA', 'ranking') }}

),

renamed as (

    select
        team_id,
        league_id,
        season_id,
        standingsdate,
        conference,
        team,
        g,
        w,
        l,
        w_pct,
        home_record,
        road_record,
        returntoplay

    from source

)

select * from renamed
