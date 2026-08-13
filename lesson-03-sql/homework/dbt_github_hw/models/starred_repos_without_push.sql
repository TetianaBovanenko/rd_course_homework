-- =====================================================================
-- TASK 5 — starred_repos_without_push (12 points). Specification: ../../MODELS.md → "starred_repos_without_push".
-- Repositories with a star (WatchEvent) but without any PushEvent:
-- anti-join using NOT EXISTS.
-- The column contract is defined below.
-- =====================================================================

SELECT DISTINCT
    watch.repo_name

FROM {{ ref('stg_events') }} AS watch

WHERE watch.event_type = 'WatchEvent'

AND NOT EXISTS (

    SELECT 1

    FROM {{ ref('stg_events') }} AS push

    WHERE push.repo_name = watch.repo_name
      AND push.event_type = 'PushEvent'

)
