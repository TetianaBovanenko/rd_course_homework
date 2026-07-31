-- =====================================================================
-- TASK 5 — starred_repos_without_push (12 балів). Специфікація: ../../MODELS.md → «starred_repos_without_push».
-- Репозиторії зі зіркою (WatchEvent), але без жодного PushEvent: anti-join (NOT EXISTS).
-- Контракт колонок нижче; заглушка повертає 0 рядків.
-- =====================================================================
{{ config(materialized='view') }}


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
