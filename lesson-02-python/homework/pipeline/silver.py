"""Silver stage — clean, filter, and de-duplicate bronze events.

Tasks 2 and 3: Implement build_silver() and write_silver_partitioned().
Contract: see CONTRACTS.md → "silver" and "silver partitioned".

build_silver():
  * Keep only event types from config.TARGET_EVENT_TYPES.
  * Remove rows with an empty or missing repo_name, or missing event_id
    or created_at.
  * Guarantee uniqueness by event_id using .unique(subset=["event_id"]).
  * Write the result to config.SILVER_FILE and return the DataFrame.

write_silver_partitioned():
  * Write silver as a Hive-partitioned dataset by event_type.
  * Directory: config.SILVER_PARTITIONED_DIR.
  * Use df.write_parquet(dir, partition_by="event_type").
"""

from __future__ import annotations

import polars as pl

from . import config


def build_silver(bronze: pl.DataFrame) -> pl.DataFrame:
    silver = (
        bronze
        .filter(
            pl.col("event_type").is_in(config.TARGET_EVENT_TYPES),
            pl.col("repo_name").is_not_null(),
            pl.col("repo_name") != "",
            pl.col("event_id").is_not_null(),
            pl.col("created_at").is_not_null(),
        )
        .unique(subset=["event_id"])
    )

    silver.write_parquet(
        config.SILVER_FILE,
        mkdir=True,
    )

    return silver


def write_silver_partitioned(silver: pl.DataFrame) -> None:
    silver.write_parquet(
        config.SILVER_PARTITIONED_DIR,
        partition_by="event_type",
        mkdir=True,
    )