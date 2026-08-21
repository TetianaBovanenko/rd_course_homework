"""Gold stage — three analytics tables built from silver events.

Tasks 4, 5, and 6: Implement the three functions below.
Contract: see CONTRACTS.md → "gold repo_activity", "gold activity_per_minute",
and "gold push_commits_by_repo".

All counters should be cast to Int64 using .cast(pl.Int64) so that the
result schema remains stable.

  * build_repo_activity: number of events and distinct event types per repo
  * build_activity_per_minute: number of events per minute
    using .dt.truncate("1m")
  * build_push_commits_by_repo: PushEvent records only — number of pushes
    and total commit_count per repo
"""

from __future__ import annotations

import polars as pl

from . import config


def build_repo_activity(silver: pl.DataFrame) -> pl.DataFrame:
    result = (
        silver
        .group_by("repo_name")
        .agg(
            pl.len().cast(pl.Int64).alias("event_count"),
            pl.col("event_type")
            .n_unique()
            .cast(pl.Int64)
            .alias("distinct_event_types"),
        )
        .sort("event_count", descending=True)
    )

    result.write_parquet(
        config.GOLD_REPO_ACTIVITY,
        mkdir=True,
    )

    return result


def build_activity_per_minute(silver: pl.DataFrame) -> pl.DataFrame:
    result = (
        silver
        .with_columns(
            pl.col("created_at")
            .dt.truncate("1m")
            .alias("minute")
        )
        .group_by("minute")
        .agg(
            pl.len().cast(pl.Int64).alias("event_count")
        )
        .sort("minute")
    )

    result.write_parquet(
        config.GOLD_ACTIVITY_PER_MINUTE,
        mkdir=True,
    )

    return result


def build_push_commits_by_repo(silver: pl.DataFrame) -> pl.DataFrame:
    result = (
        silver
        .filter(pl.col("event_type") == "PushEvent")
        .group_by("repo_name")
        .agg(
            pl.len().cast(pl.Int64).alias("push_events"),
            pl.col("commit_count")
            .sum()
            .cast(pl.Int64)
            .alias("total_commits"),
        )
    )

    result.write_parquet(
        config.GOLD_PUSH_COMMITS,
        mkdir=True,
    )

    return result