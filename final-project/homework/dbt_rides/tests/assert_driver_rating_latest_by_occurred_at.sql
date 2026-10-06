{{ config(tags=['reconcile']) }}
-- latest_rating водія мусить відповідати ride_accepted з найбільшим occurred_at,
-- незалежно від порядку прибуття (запізніла подія зі старим occurred_at не має перемагати).
with expected as (
    select distinct on (driver_key)
        driver_key,
        rating as expected_rating,
        vehicle_type as expected_vehicle_type
    from (
        select
            payload -> 'driver' ->> 'id'                       as driver_key,
            (payload -> 'driver' ->> 'rating')::numeric(4, 2)  as rating,
            payload -> 'driver' -> 'vehicle' ->> 'type'        as vehicle_type,
            occurred_at
        from {{ ref('events') }}
        where event_type = 'ride_accepted'
          and payload -> 'driver' ->> 'id' is not null
    ) e
    order by driver_key, occurred_at desc nulls last
)
select d.driver_key, d.latest_rating, d.vehicle_type,
       e.expected_rating, e.expected_vehicle_type
from {{ ref('dim_driver') }} d
join expected e using (driver_key)
where d.latest_rating is distinct from e.expected_rating
   or d.vehicle_type  is distinct from e.expected_vehicle_type
