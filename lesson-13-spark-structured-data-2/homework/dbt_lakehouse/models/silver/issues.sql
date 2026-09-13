-- Step 4: silver.issues. Specification: ../../SPEC.md → "Step 4".

-- Source: {{ ref('events') }}, types IssuesEvent AND IssueCommentEvent (both carry issue{}).

-- from_json(payload, ISSUE_SCHEMA), ISSUE_SCHEMA = var('issue_schema').

-- Grain: one row per (repo_name, issue_number) — state from the latest event by time.

-- Columns: repo_name, issue_number, title, author_login, state, opened_at, closed_at,

--          comments, label_names, comment_events_seen, last_event_at, hours_to_close

with parsed_events as (

    select
        event_id,
        repo_name,
        event_type,
        created_at as event_at,
        from_json(payload, '{{ var("issue_schema") }}') as issue_data
    from {{ ref('events') }}
    where event_type in ('IssuesEvent', 'IssueCommentEvent')

),

prepared as (

    select
        event_id,
        repo_name,
        event_type,
        event_at,
        issue_data.issue.number as issue_number,
        issue_data.issue.title as title,
        issue_data.issue.user.login as author_login,
        issue_data.issue.state as state,
        to_timestamp(issue_data.issue.created_at) as opened_at,
        to_timestamp(issue_data.issue.closed_at) as closed_at,
        issue_data.issue.comments as comments,
        transform(
            issue_data.issue.labels,
            label -> label.name
        ) as label_names
    from parsed_events
    where issue_data.issue.number is not null

),

with_comment_counts as (

    select
        *,
        sum(
            case
                when event_type = 'IssueCommentEvent' then 1
                else 0
            end
        ) over (
            partition by repo_name, issue_number
        ) as comment_events_seen
    from prepared

),

ranked as (

    select
        *,
        row_number() over (
            partition by repo_name, issue_number
            order by event_at desc, event_id desc
        ) as row_num
    from with_comment_counts

)

select
    repo_name,
    issue_number,
    title,
    author_login,
    state,
    opened_at,
    closed_at,
    comments,
    label_names,
    cast(comment_events_seen as bigint) as comment_events_seen,
    event_at as last_event_at,
    case
        when closed_at is not null
            then cast(
                (
                    unix_timestamp(closed_at)
                    - unix_timestamp(opened_at)
                ) / 3600.0
                as double
            )
        else null
    end as hours_to_close
from ranked
where row_num = 1

