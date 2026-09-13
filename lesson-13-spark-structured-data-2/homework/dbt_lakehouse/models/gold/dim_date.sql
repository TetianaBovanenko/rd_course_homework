-- Step 7: gold.dim_date. Specification: ../../SPEC.md → "Step 7".

-- Generated as a continuous calendar (NO seed): explode(sequence(min, max, interval 1 day)).

-- Min/max boundaries come from a subquery over actual dates from {{ ref('commits') }}, {{ ref('pull_requests') }},

-- {{ ref('issues') }} (pushed_at / opened_at / merged_at / closed_at). Do not hardcode.

-- Columns: date_id (int yyyyMMdd), date_day (date), day_of_week, is_weekend, iso_week, year.

with all_dates as (

    select to_date(pushed_at) as date_day
    from {{ ref('commits') }}
    where pushed_at is not null

    union all

    select to_date(opened_at) as date_day
    from {{ ref('pull_requests') }}
    where opened_at is not null

    union all

    select to_date(merged_at) as date_day
    from {{ ref('pull_requests') }}
    where merged_at is not null

    union all

    select to_date(opened_at) as date_day
    from {{ ref('issues') }}
    where opened_at is not null

    union all

    select to_date(closed_at) as date_day
    from {{ ref('issues') }}
    where closed_at is not null

),

date_bounds as (

    select
        min(date_day) as min_date,
        max(date_day) as max_date
    from all_dates

),

calendar as (

    select
        explode(
            sequence(
                min_date,
                max_date,
                interval 1 day
            )
        ) as date_day
    from date_bounds

)

select
    cast(date_format(date_day, 'yyyyMMdd') as int) as date_id,
    date_day,
    dayofweek(date_day) as day_of_week,
    dayofweek(date_day) in (1, 7) as is_weekend,
    weekofyear(date_day) as iso_week,
    year(date_day) as year
from calendar

