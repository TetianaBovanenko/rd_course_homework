-- Step 2: silver.commits. Specification: ../../SPEC.md → "Step 2".

-- Source: {{ ref('events') }}, only PushEvent.

-- from_json(payload, PUSH_SCHEMA) → explode commits array → commit grain. PUSH_SCHEMA = var('push_schema').

-- Deduplication: one row per commit_sha, keeping the earliest pushed_at.

-- Columns: commit_sha, repo_name, pushed_by, branch, author_name, author_email, message,

--          is_distinct, pushed_at, is_merge_commit, message_subject, message_length

-- Pitfall: `distinct` is a reserved word, so backticks are required in the DDL schema and field access.

with parsed_events as (

    select
        event_id,
        repo_name,
        actor_login,
        created_at as pushed_at,
        split(payload, '') as payload_parts,
        from_json(payload, '{{ var("push_schema") }}') as push_data
    from {{ ref('events') }}
    where event_type = 'PushEvent'

),

exploded_commits as (

    select
        event_id,
        repo_name,
        actor_login as pushed_by,
        pushed_at,
        push_data.ref as ref,
        commit_data.sha as commit_sha,
        commit_data.author.name as author_name,
        commit_data.author.email as author_email,
        commit_data.message as message,
        commit_data.`distinct` as is_distinct
    from parsed_events
    lateral view explode(push_data.commits) exploded as commit_data

),

prepared as (

    select
        commit_sha,
        repo_name,
        pushed_by,
        regexp_replace(ref, '^refs/heads/', '') as branch,
        author_name,
        author_email,
        message,
        is_distinct,
        pushed_at,
        startswith(message, 'Merge ') as is_merge_commit,
        split(message, '\n')[0] as message_subject,
        length(message) as message_length,
        event_id
    from exploded_commits
    where commit_sha is not null

),

deduplicated as (

    select
        *,
        row_number() over (
            partition by commit_sha
            order by pushed_at, event_id
        ) as row_num
    from prepared

)

select
    commit_sha,
    repo_name,
    pushed_by,
    branch,
    author_name,
    author_email,
    message,
    is_distinct,
    pushed_at,
    is_merge_commit,
    message_subject,
    message_length
from deduplicated
where row_num = 1


