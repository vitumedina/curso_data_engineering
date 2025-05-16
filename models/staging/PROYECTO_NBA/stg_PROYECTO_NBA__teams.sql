with 

source as (

    select * from {{ source('PROYECTO_NBA', 'teams') }}

),

renamed as (

    select
        league_id,
        team_id,
        min_year,
        max_year,
        abbreviation,
        nickname,
        yearfounded,
        city,
        arena,
        arenacapacity,
        owner,
        generalmanager,
        headcoach,
        dleagueaffiliation

    from source

)

select * from renamed
