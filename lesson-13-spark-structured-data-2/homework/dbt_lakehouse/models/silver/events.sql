{{ config(materialized='incremental', incremental_strategy='append') }}

-- Step 1: silver.events. Specification: ../../SPEC.md → "Step 1".

-- Source: {{ source('bronze', 'raw_events') }}. Keep payload as a raw string for steps 2–4.

-- Columns: event_id, event_type, actor_login, repo_name, repo_owner, created_at,

--          payload, _ingested_at, _source_file

-- Filters: 6 event types; public = true (discard NULL); event_id/repo_name/created_at not null; deduplicate by event_id.

-- Incremental (append): in the is_incremental() branch, take only rows with _ingested_at > max(_ingested_at) in {{ this }}.

with source_events as (

    select
        id as event_id,
        type as event_type,
        actor.login as actor_login,
        repo.name as repo_name,
        split(repo.name, '/')[0] as repo_owner,
        public,
        to_timestamp(created_at) as created_at,
        payload,
        _ingested_at,
        _source_file
    from {{ source('bronze', 'raw_events') }}

    {% if is_incremental() %}
    where _ingested_at > (
        select max(_ingested_at)
        from {{ this }}
    )
    {% endif %}

),

filtered as (

    select *
    from source_events
    where event_type in (
        'PushEvent',
        'PullRequestEvent',
        'IssuesEvent',
        'IssueCommentEvent',
        'WatchEvent',
        'ForkEvent'
    )
    and public = true
    and event_id is not null
    and repo_name is not null
    and created_at is not null

),

deduplicated as (

    select
        *,
        row_number() over (
            partition by event_id
            order by _ingested_at, _source_file
        ) as row_num
    from filtered

)

select
    event_id,
    event_type,
    actor_login,
    repo_name,
    repo_owner,
    created_at,
    payload,
    _ingested_at,
    _source_file
from deduplicated
where row_num = 1

