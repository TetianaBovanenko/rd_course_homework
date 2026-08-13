-- =====================================================================
-- TASK 3 — daily_activity (12 points). Specification: ../../MODELS.md → "daily_activity".
-- Number of events per day + running total using SUM(...) OVER (ORDER BY ...).
-- Column contract is defined below.
-- =====================================================================
WITH daily_events AS (

    SELECT
        event_date,
        COUNT(*) AS events
    FROM {{ ref('stg_events') }}
    GROUP BY event_date

)

SELECT
    event_date,
    events,
    SUM(events) OVER (
        ORDER BY event_date
        ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    ) AS running_events

FROM daily_events
