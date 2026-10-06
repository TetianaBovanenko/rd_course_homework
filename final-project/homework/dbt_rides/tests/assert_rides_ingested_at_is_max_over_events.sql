-- silver.rides._ingested_at = максимум _ingested_at усіх подій поїздки. Якщо взяти лише
-- ride_requested._ingested_at, запізніла payment_captured не долетить до Gold.
with expected as (
    select ride_id, max(_ingested_at) as expected
    from {{ ref('events') }}
    where ride_id is not null
    group by ride_id
)
select r.ride_id, r._ingested_at, e.expected
from {{ ref('rides') }} r
join expected e using (ride_id)
where r._ingested_at <> e.expected
