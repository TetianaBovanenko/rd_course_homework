-- =====================================================================
-- TASK 2 — repo_top_events (12 points). Specification: ../../MODELS.md → "repo_top_events".
-- Top 5 repositories by event count within each event_type using ROW_NUMBER() + QUALIFY.
-- The column contract is defined below.
-- =====================================================================

WITH repo_events AS (

    SELECT
        event_type,
        repo_name,
        COUNT(*) AS event_count
    FROM {{ ref('stg_events') }}
    GROUP BY
        event_type,
        repo_name

)

SELECT
    event_type,
    repo_name,
    event_count,
    ROW_NUMBER() OVER (
        PARTITION BY event_type
        ORDER BY event_count DESC, repo_name
    ) AS type_rank

FROM repo_events

QUALIFY type_rank <= 5