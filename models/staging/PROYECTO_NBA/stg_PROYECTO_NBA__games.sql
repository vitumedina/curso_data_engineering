with 

source as (

    select * from {{ source('PROYECTO_NBA', 'games') }}

),

renamed as (

    select
        TO_DATE(game_date_est) AS game_date_est,
        game_id::NUMERIC(25,0) AS game_id,
        ROW_NUMBER() OVER(PARTITION BY GAME_ID ORDER BY GAME_DATE_EST ASC) AS NUM_COINCIDENCIAS,
        game_status_text::VARCHAR(25) as game_status,
        -- home_team_id,
        -- visitor_team_id,
        md5(CAST(season AS TEXT)) AS season_id,
        season::NUMERIC(25,0) AS season_desc,
        team_id_home::NUMERIC(25,0) AS team_id_home,
        pts_home::NUMERIC(25,0) AS pts_home,
        fg_pct_home::NUMERIC(25,3) AS fg_pct_home,
        ft_pct_home::NUMERIC(25,3) AS ft_pct_home,
        fg3_pct_home::NUMERIC(25,3) AS fg3_pct_home,
        ast_home::NUMERIC(25,0) AS ast_home,
        reb_home::NUMERIC(25,0) AS reb_home,
        team_id_away::NUMERIC(25,0) AS team_id_away,
        pts_away::NUMERIC(25,0) AS pts_away,
        fg_pct_away::NUMERIC(25,3) AS fg_pct_away,
        ft_pct_away::NUMERIC(25,3) AS ft_pct_away,
        fg3_pct_away::NUMERIC(25,3) AS fg3_pct_away,
        ast_away::NUMERIC(25,0) AS ast_away,
        reb_away::NUMERIC(25,0) AS reb_away,
        home_team_wins::BOOLEAN AS home_team_wins

    from source
    QUALIFY NUM_COINCIDENCIAS = 1
)

select * exclude(NUM_COINCIDENCIAS) from renamed


