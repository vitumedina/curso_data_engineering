{{
    config(
        materialized = "table"
    )
}}

with dates_raw as (
        {{ dbt_date.get_date_dimension("1949-01-01", "2100-12-31") }}
)

/* extracting some date information and renaming some columns to Portuguese */
    , days_info as (
        select 
            cast(date_day as date) as fecha
            , extract(dayofweek from date_day) as dia_num
            , month(date_day) as mes
            , extract(quarter from date_day) as trimestre
            , extract(dayofyear from date_day) as dia_anho
            , year(date_day) as anho
            , case
                when month(date_day)<10 then CONCAT(TO_VARCHAR(year(date_day)-1), '/', TO_VARCHAR(RIGHT(year(date_day),2)))
                else CONCAT(TO_VARCHAR(year(date_day)), '/', TO_VARCHAR(RIGHT(year(date_day)+1,2))) 
                end as season
        from dates_raw
    )

/* renaming the column meanings, translating them into Portuguese */
    , days_named as (
        select
            *
            , case
                when dia_num = 1 then 'Domingo'
                when dia_num = 2 then 'Lunes'
                when dia_num = 3 then 'Martes'
                when dia_num = 4 then 'Miércoles'
                when dia_num = 5 then 'Jueves'
                when dia_num = 6 then 'Viernes'
                else 'Sábado' 
            end as dia_desc
            , case
                when mes = 1 then 'Enero'
                when mes = 2 then 'Febrero'
                when mes = 3 then 'Marzo'
                when mes = 4 then 'Abril'
                when mes = 5 then 'Mayo'
                when mes = 6 then 'Junio'
                when mes = 7 then 'Julio'
                when mes = 8 then 'Agosto'
                when mes = 9 then 'Septiembre'
                when mes = 10 then 'Octubre'
                when mes = 11 then 'Noviembre'
                else 'Diciembre' 
            end as mes_desc
            , case
                when mes = 1 then 'Ene'
                when mes = 2 then 'Feb'
                when mes = 3 then 'Mar'
                when mes = 4 then 'Abr'
                when mes = 5 then 'May'
                when mes = 6 then 'Jun'
                when mes = 7 then 'Jul'
                when mes = 8 then 'Ago'
                when mes = 9 then 'Sept'
                when mes = 10 then 'Oct'
                when mes = 11 then 'Nov'
                else 'Dic' 
            end as abrev_mes
            , case
                when trimestre = 1 then '1º Trimestre'
                when trimestre = 2 then '2º Trimestre'
                when trimestre = 3 then '3º Trimestre'
                else '4º Trimestre' 
            end as trimestre_desc
            , case
                when trimestre in(1,2) then 1
                else 2
            end as semestre
            , case
                when trimestre in(1,2) then '1º Semestre'
                else '2º Semestre'
            end as semestre_desc
        from days_info
    )

/* rearranging the columns */
    , final_cte as (
        select 
            fecha
            , md5(cast(fecha as TEXT)) as fecha_id
            , dia_num
            , dia_desc
            , mes
            , mes_desc
            , abrev_mes
            , trimestre
            , trimestre_desc
            , semestre
            , semestre_desc
            , dia_anho
            , anho
            , season
        from days_named
    )

select *
from final_cte