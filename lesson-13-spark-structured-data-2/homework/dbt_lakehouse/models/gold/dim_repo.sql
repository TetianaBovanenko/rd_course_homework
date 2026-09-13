-- Step 5: gold.dim_repo. Specification: ../../SPEC.md → "Step 5".

-- Source: {{ ref('events') }}. Grain: one row per repository.

-- Columns: repo_id (md5(repo_name)), repo_name, repo_owner, first_seen_at, last_seen_at,

--          event_count, is_forked (at least one ForkEvent for this repository).

with repo_stats as (

    select
        md5(repo_name) as repo_id,
        repo_name,
        repo_owner,
        min(created_at) as first_seen_at,
        max(created_at) as last_seen_at,
        count(*) as event_count,
        max(
            case
                when event_type = 'ForkEvent' then 1
                else 0
            end
        ) = 1 as is_forked
    from {{ ref('events') }}
    group by
        repo_name,
        repo_owner

)

select
    repo_id,
    repo_name,
    repo_owner,
    first_seen_at,
    last_seen_at,
    event_count,
    is_forked
from repo_stats

