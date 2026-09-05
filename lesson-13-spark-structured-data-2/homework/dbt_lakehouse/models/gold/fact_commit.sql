-- Step 8: gold.fact_commit. Specification: ../../SPEC.md → "Step 8".

-- Source: {{ ref('commits') }}. Grain does not change (1 row = 1 commit_sha).

-- FK columns: repo_id = md5(repo_name), pusher_id = md5(pushed_by),

--              date_id = cast(date_format(pushed_at,'yyyyMMdd') as int) — the same expression as in the dimensions.

-- Columns: commit_sha, repo_id, pusher_id, date_id, branch, is_merge_commit, is_distinct, message_length.

select
    commit_sha,
    md5(repo_name) as repo_id,
    md5(pushed_by) as pusher_id,
    cast(date_format(pushed_at, 'yyyyMMdd') as int) as date_id,
    branch,
    is_merge_commit,
    is_distinct,
    message_length
from {{ ref('commits') }}

