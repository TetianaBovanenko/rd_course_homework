-- Step 6: gold.dim_actor. Specification: ../../SPEC.md → "Step 6".

-- Source: {{ ref('events') }}, actor_login is not null. Grain: one row per actor.

-- Columns: actor_id (md5(actor_login)), actor_login, is_bot (ends with [bot]),

--          first_seen_at, last_seen_at, event_count, distinct_repos.

with actor_stats as (

    select
        md5(actor_login) as actor_id,
        actor_login,
        endswith(actor_login, '[bot]') as is_bot,
        min(created_at) as first_seen_at,
        max(created_at) as last_seen_at,
        count(*) as event_count,
        count(distinct repo_name) as distinct_repos
    from {{ ref('events') }}
    where actor_login is not null
    group by actor_login

)

select
    actor_id,
    actor_login,
    is_bot,
    first_seen_at,
    last_seen_at,
    event_count,
    distinct_repos
from actor_stats

