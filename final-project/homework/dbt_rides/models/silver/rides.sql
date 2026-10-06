--silver.rides — ЕТАП 2. Grain: одна поїздка (ride_id). SPEC.md, розділ 4.3.
--incremental, unique_key='ride_id', delete+insert
--перебудовуйте поїздки, яких торкнулися НОВІ події, з УСІЄЇ їхньої історії в ref('events')
--поїздка існує, коли прийшла її ride_requested; порядок прибуття подій байдужий
--status, фактична зона (перекриває заявлену), wait_seconds, trip_seconds, суми з ride_completed
--_ingested_at = максимум _ingested_at усіх подій поїздки

{{ config(
    materialized='incremental',
    unique_key='ride_id',
    incremental_strategy='delete+insert'
) }}

with events as (

    select *
    from {{ ref('events') }}
    where ride_id is not null

    {% if is_incremental() %}
      and ride_id in (
          select distinct ride_id
          from {{ ref('events') }}
          where _ingested_at > {{ high_watermark() }}
            and ride_id is not null
      )
    {% endif %}

),

requested as (

    select
        ride_id,
        occurred_at                                           as requested_at,
        payload -> 'rider' ->> 'id'                           as rider_id,
        payload -> 'rider' ->> 'platform'                     as rider_platform,
        payload -> 'rider' ->> 'app_version'                  as app_version,
        payload ->> 'requested_vehicle'                       as requested_vehicle,
        (payload ->> 'surge_estimate')::numeric               as surge_estimate,
        (payload -> 'pickup'  ->> 'zone_id')::int             as declared_pickup_zone_id,
        (payload -> 'dropoff' ->> 'zone_id')::int             as declared_dropoff_zone_id,
        _ingested_at
    from events
    where event_type = 'ride_requested'

),

accepted as (

    select
        ride_id,
        occurred_at                                           as accepted_at,
        payload -> 'driver' ->> 'id'                          as driver_id,
        _ingested_at
    from events
    where event_type = 'ride_accepted'

),

started as (

    select
        ride_id,
        occurred_at                                           as started_at,
        payload -> 'driver' ->> 'id'                          as driver_id,
        (payload #>> '{pickup,zone_id}')::int                   as actual_pickup_zone_id,
        _ingested_at
    from events
    where event_type = 'ride_started'

),

completed as (

    select
        ride_id,
        occurred_at                                           as completed_at,
        payload -> 'driver' ->> 'id'                          as driver_id,
        (payload #>> '{dropoff,zone_id}')::int                  as actual_dropoff_zone_id,
        (payload ->> 'distance_km')::numeric(8, 2)            as distance_km,
        (payload -> 'fare' ->> 'amount')::numeric(10, 2)      as fare_amount,
        (payload -> 'fare' ->> 'surge_multiplier')::numeric(4, 2) as surge_multiplier,
        (payload -> 'fare' ->> 'tolls')::numeric(10, 2)       as tolls_amount,
        (payload -> 'fare' ->> 'tip')::numeric(10, 2)         as tip_amount,
        (payload -> 'fare' ->> 'total')::numeric(10, 2)       as total_amount,
        payload -> 'fare' ->> 'currency'                      as currency,
        _ingested_at
    from events
    where event_type = 'ride_completed'

),

cancelled as (

    select
        ride_id,
        occurred_at                                           as cancelled_at,
        payload ->> 'cancelled_by'                            as cancelled_by,
        payload ->> 'reason'                                  as cancel_reason,
        payload ->> 'stage'                                   as cancel_stage,
        _ingested_at
    from events
    where event_type = 'ride_cancelled'

),

paid as (

    select
        ride_id,
        occurred_at                                           as paid_at,
        payload -> 'payment' ->> 'method'                     as payment_method,
        payload -> 'payment' ->> 'psp_reference'              as psp_reference,
        (payload -> 'payment' ->> 'amount')::numeric(10, 2)   as payment_amount,
        _ingested_at
    from events
    where event_type = 'payment_captured'

),

combined as (

    select
        r.ride_id,
        r.rider_id,
        r.rider_platform,
        r.app_version,
        r.requested_vehicle,
        coalesce(s.driver_id, c.driver_id, a.driver_id)       as driver_id,
        r.requested_at,
        a.accepted_at,
        s.started_at,
        c.completed_at,
        x.cancelled_at,
        p.paid_at,
        coalesce(s.actual_pickup_zone_id,  r.declared_pickup_zone_id)   as pickup_zone_id,
        coalesce(c.actual_dropoff_zone_id, r.declared_dropoff_zone_id)  as dropoff_zone_id,
        r.surge_estimate,
        c.distance_km,
        c.surge_multiplier,
        c.fare_amount,
        c.tolls_amount,
        c.tip_amount,
        c.total_amount,
        c.currency,
        x.cancelled_by,
        x.cancel_reason,
        x.cancel_stage,
        p.payment_method,
        p.psp_reference,
        p.payment_amount,
        greatest(
            r._ingested_at,
            coalesce(a._ingested_at, '-infinity'::timestamptz),
            coalesce(s._ingested_at, '-infinity'::timestamptz),
            coalesce(c._ingested_at, '-infinity'::timestamptz),
            coalesce(x._ingested_at, '-infinity'::timestamptz),
            coalesce(p._ingested_at, '-infinity'::timestamptz)
        )                                                     as _ingested_at
    from requested r
    left join accepted  a on a.ride_id = r.ride_id
    left join started   s on s.ride_id = r.ride_id
    left join completed c on c.ride_id = r.ride_id
    left join cancelled x on x.ride_id = r.ride_id
    left join paid      p on p.ride_id = r.ride_id

),

final as (

    select
        ride_id,
        rider_id,
        rider_platform,
        app_version,
        requested_vehicle,
        driver_id,
        case
            when completed_at is not null then 'completed'
            when cancelled_at is not null then 'cancelled'
            when started_at   is not null then 'in_progress'
            when accepted_at  is not null then 'accepted'
            else 'requested'
        end                                                   as status,
        requested_at,
        accepted_at,
        started_at,
        completed_at,
        cancelled_at,
        paid_at,
        pickup_zone_id,
        dropoff_zone_id,
        case
            when accepted_at is not null and requested_at is not null
            then extract(epoch from (accepted_at - requested_at))::int
        end                                                   as wait_seconds,
        case
            when completed_at is not null and started_at is not null
            then extract(epoch from (completed_at - started_at))::int
        end                                                   as trip_seconds,
        surge_estimate,
        distance_km,
        surge_multiplier,
        fare_amount,
        tolls_amount,
        tip_amount,
        total_amount,
        currency,
        cancelled_by,
        cancel_reason,
        cancel_stage,
        payment_method,
        psp_reference,
        payment_amount,
        _ingested_at,
        '{{ run_started_at }}'::timestamptz                   as _loaded_at
    from combined

)

select * from final