-- Test: total commits in the daily activity rollup must match fact_commit.

with daily_commits as (

    select
        sum(commits) as total_commits
    from {{ ref('fact_repo_activity_daily') }}

),

fact_commits as (

    select
        count(*) as total_commits
    from {{ ref('fact_commit') }}

)

select
    daily_commits.total_commits as daily_total,
    fact_commits.total_commits as fact_total
from daily_commits
cross join fact_commits
where daily_commits.total_commits != fact_commits.total_commits

