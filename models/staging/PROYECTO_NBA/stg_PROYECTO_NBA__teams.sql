with 

source as (

    select * from {{ source('PROYECTO_NBA', 'teams') }}

),

renamed as (

    select
        league_id::NUMERIC(25,0) AS league_id,
        team_id::NUMERIC(25,0) AS team_id,
        min_year::NUMERIC(25,0) AS min_year,
        max_year::NUMERIC(25,0) AS max_year,
        abbreviation::VARCHAR(50) AS abbreviation,
        nickname::VARCHAR(50) AS nickname,
        yearfounded::NUMERIC(25,0) AS yearfounded,
        city::VARCHAR(50) AS city,
        arena::VARCHAR(50) AS arena,
        arenacapacity::NUMERIC(25,0) AS arenacapacity,
        owner::VARCHAR(50) AS owner,
        generalmanager::VARCHAR(50) AS generalmanager,
        headcoach::VARCHAR(50) AS headcoach,
        dleagueaffiliation::VARCHAR(50) AS dleagueaffiliation

    from source

)

select * from renamed
