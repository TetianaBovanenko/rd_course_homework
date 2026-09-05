-- Step 9: gold.fact_pull_request. Specification: ../../SPEC.md → "Step 9".

-- Source: {{ ref('pull_requests') }}. Grain: PR.

-- pr_id = md5(concat_ws('|', repo_name, cast(pr_number as string)));

-- merged_date_id — NULL if not merged; label_count uses CASE (size(NULL) = -1).

-- Columns: pr_id, repo_id, author_id, opened_date_id, merged_date_id, state, is_merged, is_draft,

--          additions, deletions, churn, changed_files, commits_count, comments, review_comments,

--          hours_open, label_count.

select
    md5(
        concat_ws(
            '|',
            repo_name,
            cast(pr_number as string)
        )
    ) as pr_id,
    md5(repo_name) as repo_id,
    md5(author_login) as author_id,
    cast(date_format(opened_at, 'yyyyMMdd') as int) as opened_date_id,
    case
        when merged_at is not null
            then cast(date_format(merged_at, 'yyyyMMdd') as int)
        else null
    end as merged_date_id,
    state,
    is_merged,
    is_draft,
    additions,
    deletions,
    churn,
    changed_files,
    commits_count,
    comments,
    review_comments,
    hours_open,
    case
        when label_names is null then null
        else size(label_names)
    end as label_count
from {{ ref('pull_requests') }}

