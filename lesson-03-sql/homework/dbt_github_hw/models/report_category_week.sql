-- =====================================================================
-- TASK 7 — report_category_week (20 points). Specification: ../../MODELS.md → "report_category_week".
--
-- The report_category_week_naive.sql file is intentionally unoptimized:
-- joining to calendar using strftime(event_date) = strftime(day) transforms
-- the join key, causing DuckDB to scan all 14 partitions
-- (no predicate propagation or partition pruning).
--
-- Your task: rewrite the SAME query so that it returns IDENTICAL rows,
-- but reads only 7 partitions. See the hint in MODELS.md
-- (join using the raw partition column).
-- Check the query plan with EXPLAIN ANALYZE on the compiled model
-- and verify "Total Files Read".
-- The column contract is defined below.
-- =====================================================================
SELECT
    c.iso_week,
    cat.category,
    count(*) AS events
FROM {{ ref('stg_events') }} e
JOIN {{ ref('calendar') }} c
    ON e.event_date = c.day
JOIN {{ ref('event_categories') }} cat
    ON e.event_type = cat.event_type
WHERE c.iso_week = 2
GROUP BY c.iso_week, cat.category
ORDER BY cat.category
