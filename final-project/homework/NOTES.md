# NOTES.md

## 1. Decisions and trade-offs

### Bronze idempotency
`write_bronze` uses `df.coalesce(1).write.mode("append").jdbc(...)`, so the batch is
written through a single Spark partition and transaction boundary, providing
"all or nothing" semantics in the current implementation. Re-runs do not duplicate data
because `select_new` filters by `_source_file`, which is deterministic because the
filename encodes the partition's offset range. If the write fails, the transaction is
rolled back and the next run retries the same `_source_file`; the trade-off is that
`coalesce(1)` serializes the write and becomes a bottleneck at higher volumes.

### Silver increment
The incremental boundary is `_ingested_at`, because it records when the pipeline actually
received the event. Filtering by `occurred_at` would incorrectly drop late-arriving
events: an event can have an old `occurred_at` but still be new to the pipeline. The
`high_watermark()` macro reads `max(_ingested_at)` from the target table.

### silver.rides and fact_ride
First, we identify `ride_id`s affected by events where `_ingested_at > watermark`, then
rebuild those rides from their complete event history without applying the incremental
filter to the aggregation. This is necessary because a late event such as
`payment_captured` can update a ride created several batches earlier. `fact_ride` then
detects the rebuilt rides through the updated `_ingested_at` in `silver.rides`.

### agg_zone_hourly
We recompute the entire `requested_hour` touched by new events, rather than only
individual `(hour, zone)` buckets. If a late event changes a ride's pickup zone, the ride
moves from the old zone bucket to the new one while `requested_hour` stays unchanged;
therefore the old bucket must also be removed. A `pre_hook` deletes all rows for the
touched hours before the new aggregate is inserted.

### dim_zone — table, dim_driver — incremental
`dim_zone` is a small static seed (265 zones plus the `-1` unknown member), so rebuilding
it on every run is cheap and guarantees completeness. `dim_driver` grows from the stream
and stores the current rating using SCD Type 1, so it is incremental. If the rating were
taken only from the new batch, a late `ride_accepted` event could overwrite a newer
rating; therefore `latest_rating` is selected from the driver's full history using
`row_number() over (partition by driver_key order by occurred_at desc)`.

### --indirect-selection cautious
Without `cautious`, dbt's default `eager` selection can pull ancestors into a selected
run, so the Gold step could also rebuild Silver and break the intended
Bronze → Silver → Gold order. `cautious` also means a test whose parents span different
layers only runs when all its parents are included. Therefore
`assert_driver_rating_latest_by_occurred_at` is tagged `reconcile` and executed in the
dedicated reconciliation step; otherwise cross-layer tests could run in the wrong DAG
step or fail because a relation does not yet exist.

### Why the DAG does not use data_interval_start/end
Processing state is stored in the data itself: `_source_file` identifies loaded Bronze
files and `_ingested_at` identifies events already processed by Silver. This is
preferable to schedule-based windows because `occurred_at` and arrival time can differ
for late events. A window based on the DAG schedule could either drop late events or
process overlapping data more than once.

### 100× larger volume
Bronze would likely become the first bottleneck because `coalesce(1)` serializes the
JDBC write; I would replace it with parallel writes to a staging table followed by a
transactional `INSERT ... SELECT`. In dbt/Postgres, I would consider partitioning large
tables and replacing expensive `delete + insert` operations with `MERGE` or
partition-level replacement where appropriate. Kafka's three partitions would also limit
consumer parallelism, so I would increase partitions while retaining `ride_id` as the
message key to preserve ordering for a ride.

## 2. AI usage journal

### Case 1: `_source_file` returned a `file://` URI
- **What I asked:** An implementation of `read_landing` that creates `_source_file`
  relative to `landing/`.
- **What I got:** `F.regexp_replace(F.input_file_name(), f"^{landing}/", "")`.
- **What was wrong:** `input_file_name()` returned a URI such as
  `file:///private/var/.../landing/dt=2024-01-15/hour=.../part-....ndjson`, so the regex
  did not match the beginning of the string.
- **How I found it:** `pytest tests/test_bronze.py::test_source_file_is_relative_to_landing`
  failed with `AssertionError: file:///private/var/...`, while the expected value was
  `dt=2024-01-15/hour=...`.
- **What I did:** Added a second `regexp_replace` to normalize `file:/+` to `/` before
  stripping the landing prefix. The test then passed.

### Case 2: Incorrect incremental filter in `fact_ride`
- **What I asked:** An incremental `fact_ride` model using `_ingested_at`.
- **What I got:** `{% if is_incremental() %} and _ingested_at > {{ high_watermark() }} {% endif %}`.
- **What was wrong:** The model had no preceding `WHERE` clause, so the generated SQL
  became invalid on incremental runs. The first run passed because `is_incremental()` was
  false.
- **How I found it:** `./verify.sh transform` passed Batch 1 but Batch 2 failed with
  `syntax error at or near "and"`.
- **What I did:** Replaced `and` with `where`. This demonstrated why sequential
  incremental testing was necessary; a full-refresh run alone would not have exposed the
  problem.

### Case 3: `requested_hour` was not explicitly UTC-anchored
- **What I asked:** `fact_ride.requested_hour` representing the start of the hour in UTC.
- **What I got:** `date_trunc('hour', requested_at at time zone 'UTC')`.
- **What was wrong:** `at time zone 'UTC'` returns a `timestamp without time zone`, so
  `date_trunc` also produced a timezone-less timestamp. Under a non-UTC PostgreSQL session
  timezone, comparisons against `timestamptz` values could therefore interpret the value
  incorrectly.
- **How I found it:** `./verify.sh transform` with `PGTZ=Pacific/Chatham` caused
  `assert_dates_are_utc` to report `FAIL 393`.
- **What I did:** Added an outer `at time zone 'UTC'`:
  `date_trunc('hour', requested_at at time zone 'UTC') at time zone 'UTC'`. The test then
  passed.

## 3. My dbt tests

### Test 1
- **File:** `dbt_rides/tests/assert_driver_rating_latest_by_occurred_at.sql`
- **What it checks:** Verifies that `gold.dim_driver.latest_rating` matches the
  `ride_accepted` event with the greatest `occurred_at` for each driver, regardless of
  arrival order. The expected value is independently derived from `silver.events` and
  compared with `dim_driver`.
- **How I verified it catches the bug:** I replaced
  `order by occurred_at desc nulls last, _ingested_at desc` with `order by _ingested_at desc`
  in `dim_driver.latest_attr`. `dbt build --selector gold --full-refresh` then returned
  `FAIL 47` (47 of 80 drivers), confirming that the test detects the incorrect ordering.

### Test 2
- **File:** `dbt_rides/tests/assert_rides_ingested_at_is_max_over_events.sql`
- **What it checks:** Verifies that `silver.rides._ingested_at` equals `max(_ingested_at)`
  across all events belonging to the ride. This ensures that late events propagate into
  downstream models such as `fact_ride` and `agg_zone_hourly`.
- **How I verified it catches the bug:** I replaced `greatest(...)` in `silver.rides` with
  `coalesce(r._ingested_at, greatest(...))`, which effectively ignored later-arriving
  events. Batch 1 still passed, but after loading Batch 2 and rebuilding Silver the test
  returned `FAIL 201`; restoring `greatest(...)` made it pass again.

## 4. What I would do next

- Replace the current Bronze `coalesce(1)` approach with parallel staging-table writes
  plus a transactional `INSERT ... SELECT`, preserving atomicity without serializing the
  batch.
- Partition `silver.events`, `fact_ride`, and `agg_zone_hourly` based on their dominant
  access and incremental-processing patterns, and evaluate more efficient replacement
  strategies than `delete + insert`.
- Add more custom tests, especially for `silver.rides.surge_multiplier`, the `dim_zone -1`
  member, and duplicate entity keys.
- Add observability for model runtime, processed rows, late-event rate, and Kafka consumer
  lag; currently the pipeline is primarily failure-driven rather than metrics-driven.
