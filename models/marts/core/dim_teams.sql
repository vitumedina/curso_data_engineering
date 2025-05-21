{{
    config(
        materialized = "table"
    )
}}

with 
    source as (

    select * from {{ ref('stg_PROYECTO_NBA__teams') }}

),

renamed as (

    select *
        -- player_name::VARCHAR(50) AS player_name,
        -- team_id::NUMERIC(25,0) AS team_id,
        -- player_id::NUMERIC(25,0) AS player_id,
        -- season::NUMERIC(25,0) AS season

    from source

)

select * from renamed
