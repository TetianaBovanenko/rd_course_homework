-- Step 3: silver.pull_requests. Specification: ../../SPEC.md → "Step 3".

-- Source: {{ ref('events') }}, only PullRequestEvent. from_json(payload, PR_SCHEMA), PR_SCHEMA = var('pr_schema').

-- Grain: one row per (repo_name, pr_number) — state from the LATEST event by time (row_number desc).

-- Columns: repo_name, pr_number, title, author_login, state, is_merged, is_draft, opened_at,

--          closed_at, merged_at, additions, deletions, changed_files, commits_count, comments,

--          review_comments, author_association, label_names, last_action, last_event_at, churn, hours_open

with parsed_events as (

    select
        event_id,
        repo_name,
        created_at as event_at,
        from_json(payload, '{{ var("pr_schema") }}') as pr_data
    from {{ ref('events') }}
    where event_type = 'PullRequestEvent'

),

prepared as (

    select
        event_id,
        repo_name,
        pr_data.number as pr_number,
        pr_data.pull_request.title as title,
        pr_data.pull_request.user.login as author_login,
        pr_data.pull_request.state as state,
        pr_data.pull_request.merged as is_merged,
        pr_data.pull_request.draft as is_draft,
        to_timestamp(pr_data.pull_request.created_at) as opened_at,
        to_timestamp(pr_data.pull_request.closed_at) as closed_at,
        to_timestamp(pr_data.pull_request.merged_at) as merged_at,
        pr_data.pull_request.additions as additions,
        pr_data.pull_request.deletions as deletions,
        pr_data.pull_request.changed_files as changed_files,
        pr_data.pull_request.commits as commits_count,
        pr_data.pull_request.comments as comments,
        pr_data.pull_request.review_comments as review_comments,
        pr_data.pull_request.author_association as author_association,
        transform(
            pr_data.pull_request.labels,
            label -> label.name
        ) as label_names,
        pr_data.action as last_action,
        event_at as last_event_at
    from parsed_events
    where pr_data.number is not null

),

ranked as (

    select
        *,
        row_number() over (
            partition by repo_name, pr_number
            order by last_event_at desc, event_id desc
        ) as row_num
    from prepared

)

select
    repo_name,
    pr_number,
    title,
    author_login,
    state,
    is_merged,
    is_draft,
    opened_at,
    closed_at,
    merged_at,
    additions,
    deletions,
    changed_files,
    commits_count,
    comments,
    review_comments,
    author_association,
    label_names,
    last_action,
    last_event_at,
    additions + deletions as churn,
    cast(
        (
            unix_timestamp(coalesce(closed_at, last_event_at))
            - unix_timestamp(opened_at)
        ) / 3600.0
        as double
    ) as hours_open
from ranked
where row_num = 1

