"""Gold stage — three analytics tables built from silver.

TODO (Завдання 4, 5, 6): реалізуйте три функції нижче.
Контракт: див. CONTRACTS.md → "gold repo_activity", "gold activity_per_minute",
"gold push_commits_by_repo". Усі лічильники приводьте до Int64 (.cast(pl.Int64)),
щоб схема результату була стабільною.

  * build_repo_activity:        кількість подій + кількість унікальних типів на repo
  * build_activity_per_minute:  кількість подій по хвилинах (.dt.truncate("1m"))
  * build_push_commits_by_repo: тільки PushEvent — кількість пушів і сума commit_count на repo
"""

from __future__ import annotations

from pathlib import Path

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

    Path(config.GOLD_REPO_ACTIVITY).parent.mkdir(
        parents=True,
        exist_ok=True,
    )

    result.write_parquet(config.GOLD_REPO_ACTIVITY)

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

    Path(config.GOLD_ACTIVITY_PER_MINUTE).parent.mkdir(
        parents=True,
        exist_ok=True,
    )

    result.write_parquet(config.GOLD_ACTIVITY_PER_MINUTE)

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

    Path(config.GOLD_PUSH_COMMITS).parent.mkdir(
        parents=True,
        exist_ok=True,
    )

    result.write_parquet(config.GOLD_PUSH_COMMITS)

    return result