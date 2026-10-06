--gold.dim_zone — ЕТАП 2. Grain: зона. SPEC.md, розділ 4.4. 
--table; джерело — ref('seed_taxi_zone'); порожні значення -> 'Unknown'; плюс член zone_key = -1

{{ config(materialized='table') }}

with seed as (

    select
        location_id::int                                                   as zone_key,
        coalesce(nullif(trim(borough),      ''), 'Unknown')                as borough,
        coalesce(nullif(trim(zone_name),    ''), 'Unknown')                as zone_name,
        coalesce(nullif(trim(service_zone), ''), 'Unknown')                as service_zone
    from {{ ref('seed_taxi_zone') }}

),

unknown_member as (

    select
        -1          as zone_key,
        'Unknown'   as borough,
        'Unknown'   as zone_name,
        'Unknown'   as service_zone

)

select * from seed
union all
select * from unknown_member
