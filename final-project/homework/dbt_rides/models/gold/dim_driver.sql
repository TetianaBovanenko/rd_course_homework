--gold.dim_driver — ЕТАП 2. Grain: водій. SPEC.md, розділ 4.4.
--incremental, unique_key='driver_key', delete+insert; за подіями ride_accepted у ref('events')
--«останній» рейтинг — за occurred_at; перераховуйте водія з УСІЄЇ його історії, не з батча 
--плюс член driver_key = 'unknown' (поїздки, скасовані до прийняття)

{{ config(
    materialized='incremental',
    unique_key='driver_key',
    incremental_strategy='delete+insert'
) }}

with accepted as (

    select
        payload -> 'driver' ->> 'id'                          as driver_key,
        (payload -> 'driver' ->> 'rating')::numeric(4, 2)     as rating,
        payload -> 'driver' -> 'vehicle' ->> 'type'           as vehicle_type,
        payload -> 'driver' -> 'vehicle' ->> 'medallion'      as medallion,
        occurred_at,
        _ingested_at
    from {{ ref('events') }}
    where event_type = 'ride_accepted'
      and payload -> 'driver' ->> 'id' is not null

),

-- on incremental runs: keep only drivers touched by new events,
-- but then rebuild each of them from ALL their history
scoped as (

    select *
    from accepted

    {% if is_incremental() %}
    where driver_key in (
        select distinct driver_key
        from accepted
        where _ingested_at > {{ high_watermark() }}
    )
    {% endif %}

),

latest_attr as (

    select
        driver_key,
        rating,
        vehicle_type,
        medallion
    from (
        select
            *,
            row_number() over (
                partition by driver_key
                order by occurred_at desc nulls last, _ingested_at desc
            ) as rn
        from scoped
    ) t
    where rn = 1

),

bounds as (

    select
        driver_key,
        min(occurred_at)  as first_seen_at,
        max(occurred_at)  as last_seen_at,
        max(_ingested_at) as _ingested_at
    from scoped
    group by driver_key

),

drivers as (

    select
        b.driver_key,
        a.vehicle_type,
        a.medallion,
        a.rating                                            as latest_rating,
        b.first_seen_at,
        b.last_seen_at,
        b._ingested_at,
        '{{ run_started_at }}'::timestamptz                 as _loaded_at
    from bounds b
    join latest_attr a on a.driver_key = b.driver_key

)

select * from drivers

union all

select
    'unknown'                                           as driver_key,
    'unknown'                                           as vehicle_type,
    null::text                                          as medallion,
    null::numeric(4, 2)                                 as latest_rating,
    null::timestamptz                                   as first_seen_at,
    null::timestamptz                                   as last_seen_at,
    '-infinity'::timestamptz                            as _ingested_at,
    '{{ run_started_at }}'::timestamptz                 as _loaded_at
