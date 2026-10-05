"""Bronze: landing-зона -> bronze.raw_events у Postgres. ЕТАП 1 — три місця нижче.

Spark у local mode читає NDJSON із landing і дописує рядки у сховище через JDBC.

Контракт шару Bronze (повністю — SPEC.md, розділ 3):
  * читаємо з ЯВНОЮ схемою (ніякого inferSchema: схема — це контракт, а не здогадка);
  * `payload` лишається СИРИМ JSON-рядком: Bronze нічого не парсить і нічого не виправляє;
  * ідемпотентність за файлом: файл, який уже завантажено, вдруге не потрапляє;
  * атомарний append: або всі нові рядки в таблиці, або жодного.

    uv run python bronze_job.py
"""

from __future__ import annotations

import re

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StringType, StructField, StructType, TimestampType

from common import config
from common.spark import build_spark

# Явна схема конверта події. `payload` оголошено як StringType: Spark віддає
# вкладений обʼєкт його текстом як є, без розбору — Bronze не парсить payload.
EVENT_SCHEMA = StructType(
    [
        StructField("event_id", StringType(), False),
        StructField("event_type", StringType(), False),
        StructField("ride_id", StringType(), True),
        StructField("source", StringType(), True),
        StructField("occurred_at", TimestampType(), True),
        StructField("payload", StringType(), True),
    ]
)


def read_landing(spark: SparkSession, landing_dir: str) -> DataFrame:
    """Сирі події з landing плюс метадані ingestion.

    Читає лише `*.ndjson` (рекурсивно); `*.ndjson.tmp` — in-flight файли consumer-а,
    їх читати не можна. `_source_file` — шлях ВІДНОСНО landing/, `_ingested_at` — момент запуску.
    `dt=` і `hour=` у шляху лишаються частиною `_source_file`, а не бізнес-колонками.
    """
    landing = landing_dir.rstrip("/")

    df = (
        spark.read.schema(EVENT_SCHEMA)
        .option("recursiveFileLookup", "true")
        .option("pathGlobFilter", "*.ndjson")
        .json(landing)
    )

    return df.select(
        "event_id",
        "event_type",
        "ride_id",
        "occurred_at",
        "source",
        "payload",
        F.regexp_replace(
            F.regexp_replace(F.input_file_name(), r"^file:/+", "/"),
            f"^{re.escape(landing)}/",
            "",
        ).alias("_source_file"),
        F.current_timestamp().alias("_ingested_at"),
    )


def loaded_files(spark: SparkSession) -> set[str]:
    """Файли, які вже лежать у Bronze (ключ ідемпотентності). ДАНО."""
    query = f"(SELECT DISTINCT _source_file FROM {config.BRONZE_TABLE}) AS t"
    rows = spark.read.jdbc(config.JDBC_URL, query, properties=config.JDBC_PROPERTIES).collect()
    return {r["_source_file"] for r in rows}


def select_new(df: DataFrame, already_loaded: set[str]) -> DataFrame:
    """Лишає рядки з файлів, яких ще нема в Bronze.

    Порожній `already_loaded` — це перший запуск (усе нове). Фільтр за `_source_file`,
    без дедуплікації за `event_id`: дублікати лишаються в Bronze, їх прибирає Silver.
    """
    if not already_loaded:
        return df
    return df.filter(~F.col("_source_file").isin(already_loaded))


def write_bronze(df: DataFrame) -> None:
    """Append у `config.BRONZE_TABLE` ОДНІЄЮ транзакцією.

    Spark пише JDBC по-партиційно: кожна партиція DataFrame — окрема транзакція.
    Щоб отримати «або все, або нічого», зводимо батч в одну партицію через `coalesce(1)` —
    тоді відкривається одна транзакція, і збій посеред запису не лишає частини батча.
    """
    (
        df.coalesce(1)
        .write.mode("append")
        .jdbc(
            config.JDBC_URL,
            config.BRONZE_TABLE,
            properties=config.JDBC_PROPERTIES,
        )
    )


def main() -> None:
    """ДАНО."""
    spark = build_spark("bronze-job")
    try:
        raw = read_landing(spark, str(config.LANDING_DIR))
        new = select_new(raw, loaded_files(spark)).cache()

        n_new = new.count()
        if n_new == 0:
            print("Bronze: нових файлів немає — пропускаю запис (ідемпотентно).")
        else:
            n_files = new.select("_source_file").distinct().count()
            write_bronze(new)
            print(f"Bronze: додано {n_new} подій із {n_files} файлів.")

        total = spark.read.jdbc(
            config.JDBC_URL,
            f"(SELECT count(*) AS n FROM {config.BRONZE_TABLE}) AS t",
            properties=config.JDBC_PROPERTIES,
        ).collect()[0]["n"]
        print(f"Bronze total: {total}")
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
