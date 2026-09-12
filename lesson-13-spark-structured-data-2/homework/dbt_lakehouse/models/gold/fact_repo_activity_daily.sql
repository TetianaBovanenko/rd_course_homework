-- Step 10: gold.fact_repo_activity_daily

with commit_base as (

    select
        md5(repo_name) as repo_id,
        cast(date_format(pushed_at, 'yyyyMMdd') as int) as date_id,
        author_email
    from {{ ref('commits') }}
    where pushed_at is not null

),

commit_activity as (

    select
        repo_id,
        date_id,
        cast(count(*) as bigint) as commits,
        cast(count(distinct author_email) as bigint) as distinct_committers,
        cast(0 as bigint) as prs_opened,
        cast(0 as bigint) as prs_merged,
        cast(0 as bigint) as issues_opened,
        cast(0 as bigint) as issues_closed,
        cast(0 as bigint) as stars,
        cast(0 as bigint) as forks
    from commit_base
    group by
        repo_id,
        date_id

),

pr_base as (

    select
        md5(repo_name) as repo_id,
        cast(date_format(opened_at, 'yyyyMMdd') as int) as opened_date_id,
        cast(date_format(merged_at, 'yyyyMMdd') as int) as merged_date_id
    from {{ ref('pull_requests') }}

),

pr_opened_activity as (

    select
        repo_id,
        opened_date_id as date_id,
        cast(0 as bigint) as commits,
        cast(0 as bigint) as distinct_committers,
        cast(count(*) as bigint) as prs_opened,
        cast(0 as bigint) as prs_merged,
        cast(0 as bigint) as issues_opened,
        cast(0 as bigint) as issues_closed,
        cast(0 as bigint) as stars,
        cast(0 as bigint) as forks
    from pr_base
    where opened_date_id is not null
    group by
        repo_id,
        opened_date_id

),

pr_merged_activity as (

    select
        repo_id,
        merged_date_id as date_id,
        cast(0 as bigint) as commits,
        cast(0 as bigint) as distinct_committers,
        cast(0 as bigint) as prs_opened,
        cast(count(*) as bigint) as prs_merged,
        cast(0 as bigint) as issues_opened,
        cast(0 as bigint) as issues_closed,
        cast(0 as bigint) as stars,
        cast(0 as bigint) as forks
    from pr_base
    where merged_date_id is not null
    group by
        repo_id,
        merged_date_id

),

issue_base as (

    select
        md5(repo_name) as repo_id,
        cast(date_format(opened_at, 'yyyyMMdd') as int) as opened_date_id,
        cast(date_format(closed_at, 'yyyyMMdd') as int) as closed_date_id
    from {{ ref('issues') }}

),

issue_opened_activity as (

    select
        repo_id,
        opened_date_id as date_id,
        cast(0 as bigint) as commits,
        cast(0 as bigint) as distinct_committers,
        cast(0 as bigint) as prs_opened,
        cast(0 as bigint) as prs_merged,
        cast(count(*) as bigint) as issues_opened,
        cast(0 as bigint) as issues_closed,
        cast(0 as bigint) as stars,
        cast(0 as bigint) as forks
    from issue_base
    where opened_date_id is not null
    group by
        repo_id,
        opened_date_id

),

issue_closed_activity as (

    select
        repo_id,
        closed_date_id as date_id,
        cast(0 as bigint) as commits,
        cast(0 as bigint) as distinct_committers,
        cast(0 as bigint) as prs_opened,
        cast(0 as bigint) as prs_merged,
        cast(0 as bigint) as issues_opened,
        cast(count(*) as bigint) as issues_closed,
        cast(0 as bigint) as stars,
        cast(0 as bigint) as forks
    from issue_base
    where closed_date_id is not null
    group by
        repo_id,
        closed_date_id

),

event_base as (

    select
        md5(repo_name) as repo_id,
        cast(date_format(created_at, 'yyyyMMdd') as int) as date_id,
        event_type,
        event_id
    from {{ ref('events') }}
    where event_type in ('WatchEvent', 'ForkEvent')
      and created_at is not null

),

event_activity as (

    select
        repo_id,
        date_id,
        cast(0 as bigint) as commits,
        cast(0 as bigint) as distinct_committers,
        cast(0 as bigint) as prs_opened,
        cast(0 as bigint) as prs_merged,
        cast(0 as bigint) as issues_opened,
        cast(0 as bigint) as issues_closed,
        cast(count(distinct case
            when event_type = 'WatchEvent' then event_id
        end) as bigint) as stars,
        cast(count(distinct case
            when event_type = 'ForkEvent' then event_id
        end) as bigint) as forks
    from event_base
    group by
        repo_id,
        date_id

),

all_activity as (

    select * from commit_activity

    union all

    select * from pr_opened_activity

    union all

    select * from pr_merged_activity

    union all

    select * from issue_opened_activity

    union all

    select * from issue_closed_activity

    union all

    select * from event_activity

),

daily_rollup as (

    select
        repo_id,
        date_id,
        sum(commits) as commits,
        sum(distinct_committers) as distinct_committers,
        sum(prs_opened) as prs_opened,
        sum(prs_merged) as prs_merged,
        sum(issues_opened) as issues_opened,
        sum(issues_closed) as issues_closed,
        sum(stars) as stars,
        sum(forks) as forks
    from all_activity
    group by
        repo_id,
        date_id
    having
        sum(commits) > 0
        or sum(distinct_committers) > 0
        or sum(prs_opened) > 0
        or sum(prs_merged) > 0
        or sum(issues_opened) > 0
        or sum(issues_closed) > 0
        or sum(stars) > 0
        or sum(forks) > 0

)

select
    md5(concat_ws('|', repo_id, cast(date_id as string))) as activity_id,
    repo_id,
    date_id,
    commits,
    distinct_committers,
    prs_opened,
    prs_merged,
    issues_opened,
    issues_closed,
    stars,
    forks
from daily_rollup

