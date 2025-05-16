with 

source as (

    select * from {{ source('PROYECTO_NBA', 'players') }}

),

renamed as (

    select
        player_name,
        team_id,
        player_id,
        season

    from source

)

select * from renamed
