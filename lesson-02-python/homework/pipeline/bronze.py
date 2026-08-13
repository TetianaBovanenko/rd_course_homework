"""Bronze stage — read raw NDJSON and flatten it into one wide table.

Task 1: Implement build_bronze().
Column and type contract: see CONTRACTS.md → "bronze".

Hints:
  * Read NDJSON lazily:
    pl.scan_ndjson(config.LANDING_FILE, schema=config.LANDING_SCHEMA)
  * Flatten nested structures with .struct.field("...")
  * Convert created_at to datetime:
    .str.to_datetime("%Y-%m-%dT%H:%M:%SZ", time_zone="UTC")
  * commit_count is the length of payload.commits. For non-PushEvent
    records, commits may be missing, so fill null values with 0.
  * Write the result to config.BRONZE_FILE as Parquet and return the
    resulting DataFrame.
"""

from __future__ import annotations

import polars as pl

from . import config


def build_bronze() -> pl.DataFrame:
    events = (
        pl.scan_ndjson(
            config.LANDING_FILE,
            schema=config.LANDING_SCHEMA,
        )
        .select(
            pl.col("id").alias("event_id"),
            pl.col("type").alias("event_type"),
            pl.col("actor").struct.field("id").alias("actor_id"),
            pl.col("actor").struct.field("login").alias("actor_login"),
            pl.col("repo").struct.field("id").alias("repo_id"),
            pl.col("repo").struct.field("name").alias("repo_name"),
            pl.col("created_at")
            .str.to_datetime(
                "%Y-%m-%dT%H:%M:%SZ",
                time_zone="UTC",
            )
            .alias("created_at"),
            pl.col("public"),
            pl.col("payload").struct.field("action").alias("action"),
            pl.col("payload")
            .struct.field("commits")
            .list.len()
            .fill_null(0)
            .cast(pl.Int64)
            .alias("commit_count"),
        )
        .collect()
    )

    events.write_parquet(
        config.BRONZE_FILE,
        mkdir=True,
    )

    return events