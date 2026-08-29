"""PySpark job over GitHub Archive — YOUR code (L12).

Specification: SPEC.md.

Implement the functions with `raise NotImplementedError`. Orchestration
(`build_spark`, `read_raw`, `main`) is already provided — it calls your
functions and writes the results to data/output/.

Run:       uv run python job.py
Verify:    uv run pytest

Run from the homework/ directory (all paths are relative to it).
"""

from __future__ import annotations

import logging
import shutil

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    BooleanType,
    StringType,
    StructField,
    StructType,
)
from pyspark.sql.window import Window


LANDING_GLOB = "data/landing/*.json.gz"
OUTPUT_DIR = "data/output"

TARGET_EVENT_TYPES = [
    "PushEvent",
    "PullRequestEvent",
    "IssuesEvent",
    "WatchEvent",
    "IssueCommentEvent",
]
SUMMARY_DIMENSIONS = ["event_type", "repo_owner", "actor_login", "hour"]
TOP_N = 5
BOT_SUFFIX = "[bot]"

log = logging.getLogger(__name__)


# ── Step 1 — reading schema ──────────────────────────────────────────────────
def event_schema() -> StructType:
    """Return the explicit schema for landing files.

    Uses schema-on-read without inferSchema.

    SPEC.md → "Step 1".
    """
    return StructType(
        [
            StructField("id", StringType(), True),
            StructField("type", StringType(), True),
            StructField(
                "actor",
                StructType(
                    [
                        StructField("login", StringType(), True),
                    ]
                ),
                True,
            ),
            StructField(
                "repo",
                StructType(
                    [
                        StructField("name", StringType(), True),
                    ]
                ),
                True,
            ),
            StructField("public", BooleanType(), True),
            StructField("created_at", StringType(), True),
        ]
    )


def read_raw(spark: SparkSession) -> DataFrame:
    """Read landing files using the explicit event schema."""
    return spark.read.schema(event_schema()).json(LANDING_GLOB)


# ── Step 2 — flattening ───────────────────────────────────────────────────────
def flatten(raw: DataFrame) -> DataFrame:
    """Flatten nested structures into six flat columns.

    SPEC.md → "Step 2".
    """
    return raw.select(
        F.col("id").alias("event_id"),
        F.col("type").alias("event_type"),
        F.col("actor.login").alias("actor_login"),
        F.col("repo.name").alias("repo_name"),
        F.col("public"),
        F.to_timestamp(F.col("created_at")).alias("created_at"),
    )


# ── Step 3 — cleaning ─────────────────────────────────────────────────────────
def clean(events: DataFrame) -> DataFrame:
    """Apply quality filters and deduplicate events.

    SPEC.md → "Step 3".
    """
    return (
        events.filter(F.col("event_type").isin(TARGET_EVENT_TYPES))
        .filter(F.col("public") == True)
        .filter(
            F.col("event_id").isNotNull()
            & F.col("repo_name").isNotNull()
            & F.col("created_at").isNotNull()
        )
        .dropDuplicates(["event_id"])
    )


# ── Step 4 — derived columns ──────────────────────────────────────────────────
def with_derived(events: DataFrame) -> DataFrame:
    """Add repo_owner, is_bot, and hour columns.

    SPEC.md → "Step 4".
    """
    return (
        events.withColumn(
            "repo_owner",
            F.split(F.col("repo_name"), "/").getItem(0),
        )
        .withColumn(
            "is_bot",
            F.coalesce(
                F.col("actor_login").endswith(BOT_SUFFIX),
                F.lit(False),
            ),
        )
        .withColumn(
            "hour",
            F.date_trunc("hour", F.col("created_at")),
        )
    )


# ── Step 5 — owner totals ─────────────────────────────────────────────────────
def owner_totals(events: DataFrame) -> DataFrame:
    """Aggregate event statistics to one row per repository owner.

    SPEC.md → "Step 5".
    """
    return events.groupBy("repo_owner").agg(
        F.count("*").alias("owner_events"),
        F.countDistinct("repo_name").alias("owner_repos"),
        F.sum(
            F.when(F.col("is_bot") == True, 1).otherwise(0)
        ).alias("owner_bot_events"),
    )


# ── Step 6 — top-N repositories within event type ─────────────────────────────
def top_repos_per_type(events: DataFrame, n: int) -> DataFrame:
    """Return the top-N repositories within each event type.

    Uses a window function with a deterministic repository-name tie-breaker.

    SPEC.md → "Step 6".
    """
    repo_counts = events.groupBy(
        "event_type",
        "repo_name",
    ).agg(
        F.count("*").alias("repo_event_count")
    )

    window = Window.partitionBy("event_type").orderBy(
        F.col("repo_event_count").desc(),
        F.col("repo_name").asc(),
    )

    return (
        repo_counts.withColumn(
            "rank",
            F.row_number().over(window),
        )
        .filter(F.col("rank") <= n)
        .select(
            "event_type",
            "repo_name",
            "repo_event_count",
            "rank",
        )
    )


# ── Step 7 — enrich top repositories ──────────────────────────────────────────
def enrich_top_repos(top_repos: DataFrame, owners: DataFrame) -> DataFrame:
    """Join top repositories with owner totals and calculate owner share.

    Uses a broadcast LEFT JOIN because the owners table is small.

    SPEC.md → "Step 7".
    """
    top = top_repos.withColumn(
        "repo_owner",
        F.split(F.col("repo_name"), "/").getItem(0),
    )

    result = top.join(
        F.broadcast(owners),
        on="repo_owner",
        how="left",
    )

    owner_events = F.coalesce(
        F.col("owner_events"),
        F.lit(0),
    )

    owner_repos = F.coalesce(
        F.col("owner_repos"),
        F.lit(0),
    )

    owner_share = F.when(
        owner_events > 0,
        F.round(
            F.col("repo_event_count") / owner_events,
            4,
        ),
    )

    return result.select(
        "event_type",
        "repo_name",
        "repo_owner",
        "repo_event_count",
        "rank",
        owner_events.alias("owner_events"),
        owner_repos.alias("owner_repos"),
        owner_share.alias("owner_share"),
    )


# ── Step 8 — one summary slice ────────────────────────────────────────────────
def summary_slice(events: DataFrame, dimension: str) -> DataFrame:
    """Build one summary slice for the specified dimension.

    The dimension name is supplied as an argument, so the same function
    works for every dimension listed in SUMMARY_DIMENSIONS.

    SPEC.md → "Step 8".
    """
    return events.groupBy(
        F.col(dimension).cast(StringType()).alias("dimension_value")
    ).agg(
        F.count("*").alias("events"),
        F.countDistinct("repo_name").alias("distinct_repos"),
    ).select(
        F.lit(dimension).alias("dimension"),
        F.col("dimension_value"),
        F.col("events"),
        F.col("distinct_repos"),
    )


# ── Step 9 — all summary slices in one table ─────────────────────────────────
def build_summary(events: DataFrame, dimensions: list[str]) -> DataFrame:
    """Build one summary table containing all requested dimensions.

    Each dimension is aggregated independently and the results are combined
    with unionByName.

    SPEC.md → "Step 9".
    """
    slices = [
        summary_slice(events, dimension)
        for dimension in dimensions
    ]

    result = slices[0]

    for current_slice in slices[1:]:
        result = result.unionByName(current_slice)

    return result


# ── Step 10 — writing marts ───────────────────────────────────────────────────
def write_outputs(outputs: dict[str, tuple[DataFrame, str | None]]) -> None:
    """Write every mart to data/output/<name>/ as Parquet.

    If no partitioning column is provided, write one Parquet file.
    If a partitioning column is provided, repartition by that column first
    and then write using partitionBy.

    SPEC.md → "Step 10".
    """
    for name, (df, partition_column) in outputs.items():
        output_path = f"{OUTPUT_DIR}/{name}"

        if partition_column is None:
            (
                df.coalesce(1)
                .write.mode("overwrite")
                .format("parquet")
                .save(output_path)
            )
        else:
            (
                df.repartition(partition_column)
                .write.mode("overwrite")
                .format("parquet")
                .partitionBy(partition_column)
                .save(output_path)
            )


# ── Orchestration (PROVIDED) ──────────────────────────────────────────────────
def build_spark(app_name: str) -> SparkSession:
    spark = (
        SparkSession.builder.master("local[*]")
        .appName(app_name)
        .config("spark.ui.enabled", "false")
        .config("spark.sql.shuffle.partitions", "4")
        # UTC — otherwise date_trunc("hour") would produce different
        # values on machines in different time zones.
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")
    return spark


def main() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s  %(levelname)-7s %(message)s",
    )
    logging.getLogger("py4j").setLevel(logging.WARNING)

    spark = build_spark("l12-github")
    shutil.rmtree(OUTPUT_DIR, ignore_errors=True)

    # events is used by several marts, so cache it instead of reading
    # the landing files four separate times.
    events = with_derived(
        clean(
            flatten(
                read_raw(spark)
            )
        )
    ).cache()

    owners = owner_totals(events)
    top_repos = enrich_top_repos(
        top_repos_per_type(events, TOP_N),
        owners,
    )
    summary = build_summary(events, SUMMARY_DIMENSIONS)

    marts: dict[str, tuple[DataFrame, str | None]] = {
        "events": (events, "event_type"),
        "owner_totals": (owners, None),
        "top_repos": (top_repos, None),
        "summary": (summary, None),
    }

    write_outputs(marts)

    for name, (df, _) in marts.items():
        log.info("%-13s %d", f"{name}:", df.count())

    events.unpersist()
    spark.stop()


if __name__ == "__main__":
    main()

