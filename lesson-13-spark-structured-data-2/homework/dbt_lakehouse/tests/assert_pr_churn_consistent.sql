-- Test: churn in fact_pull_request must equal additions + deletions.

select
    pr_id,
    churn,
    additions,
    deletions
from {{ ref('fact_pull_request') }}
where churn != additions + deletions


