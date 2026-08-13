-- =====================================================================
-- TASK 6 — mart_category_daily (20 points). Specification: ../../MODELS.md → "mart_category_daily".
-- Wide mart: multi-join of stg_events, event_categories, and calendar,
-- aggregated by day × category.
-- The column contract is defined below.
-- =====================================================================

SELECT
    e.event_date,
    c.is_weekend,
    cat.category,
    COUNT(*) AS events,
    COUNT(DISTINCT e.repo_name) AS distinct_repos,
    COUNT(DISTINCT e.actor_login) AS distinct_actors

FROM {{ ref('stg_events') }} AS e

JOIN {{ ref('event_categories') }} AS cat
    ON e.event_type = cat.event_type

JOIN {{ ref('calendar') }} AS c
    ON e.event_date = c.day

GROUP BY
    e.event_date,
    c.is_weekend,
    cat.category
