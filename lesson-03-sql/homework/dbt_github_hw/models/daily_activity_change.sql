-- =====================================================================
-- TASK 4 — daily_activity_change (12 points). Specification: ../../MODELS.md → "daily_activity_change".
-- Day-over-day change in the number of events using LAG(...) OVER (ORDER BY ...).
-- The column contract is defined below.
-- =====================================================================

{{ config(materialized='view') }}

WITH daily_events AS (

    SELECT
        event_date,
        COUNT(*) AS events
    FROM {{ ref('stg_events') }}
    GROUP BY event_date

),

daily_events_with_previous AS (

    SELECT
        event_date,
        events,
        LAG(events) OVER (
            ORDER BY event_date
        ) AS prev_day_events
    FROM daily_events

)

SELECT
    event_date,
    events,
    prev_day_events,
    events - prev_day_events AS delta_events

FROM daily_events_with_previous