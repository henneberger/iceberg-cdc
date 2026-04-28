# iceberg-cdc

[![Build](https://github.com/henneberger/iceberg-cdc/actions/workflows/build.yml/badge.svg)](https://github.com/henneberger/iceberg-cdc/actions/workflows/build.yml)
![Java 17](https://img.shields.io/badge/Java-17-007396?logo=openjdk&logoColor=white)
![Apache Flink 2.0.0](https://img.shields.io/badge/Apache%20Flink-2.0.0-E6526F?logo=apacheflink&logoColor=white)
![Apache Iceberg 1.10.1](https://img.shields.io/badge/Apache%20Iceberg-1.10.1-2E8BFF)
![Gradle](https://img.shields.io/badge/build-Gradle-02303A?logo=gradle&logoColor=white)
[![Maven Central](https://img.shields.io/maven-central/v/dev.henneberger/iceberg-cdc.svg?label=Maven%20Central)](https://central.sonatype.com/artifact/dev.henneberger/iceberg-cdc)
[![License](https://img.shields.io/badge/license-Apache--2.0-blue.svg)](LICENSE)

`iceberg-cdc` is an Apache Flink source connector that exposes Apache Iceberg tables as
CDC streams.

The connector walks Iceberg snapshot history, plans row-level changes, and emits
Flink changelog rows for inserts and deletes. It is intended for Flink SQL jobs
that need to consume Iceberg tables as unbounded sources, including temporal
joins against Iceberg-backed dimension/change-event tables.

## Supported Stack

| Component | Version |
| --- | --- |
| Apache Flink | 2.0.0 |
| Apache Iceberg | 1.10.1 |
| Java | 17 |

## What It Supports

| Capability | Status |
| --- | --- |
| Added data files as `INSERT` rows | Supported |
| Iceberg v3 deletion vectors as `DELETE` rows | Supported |
| Positional delete files as `DELETE` rows | Supported |
| Equality delete files as `DELETE` rows | Supported |
| Removed data files from overwrite snapshots | Supported |
| Compaction/rewrite snapshot filtering | Supported |
| Unbounded snapshot discovery | Supported |
| Bounded historical reads | Supported |
| Checkpointed enumerator bookmark | Supported |
| Per-split row-position recovery | Supported |
| Schema evolution for added columns | Supported |
| Projection pushdown | Supported |
| Flink SQL metadata columns | Supported |
| Global CDC ordering | Supported with `split-assignment-mode = 'ordered'` |
| File-affinity parallel split assignment | Supported with per-file ordering |
| Filter pushdown | Not implemented |
| Partition pruning | Not implemented |

## SQL Usage

```sql
CREATE TABLE events_cdc (
  id BIGINT,
  name STRING,
  amount DECIMAL(12, 2),
  event_time TIMESTAMP(3),
  WATERMARK FOR event_time AS event_time - INTERVAL '5' SECOND
) WITH (
  'connector' = 'iceberg-cdc',
  'catalog-impl' = 'org.apache.iceberg.rest.RESTCatalog',
  'catalog.uri' = 'https://catalog.example.com',
  'warehouse' = 's3://warehouse',
  'table' = 'app.events',
  'discovery-interval' = '5 s',
  'scan.startup.mode' = 'latest',
  'bounded' = 'false',
  'split-assignment-mode' = 'ordered'
);

SELECT * FROM events_cdc;
```

The connector emits a Flink changelog stream:

| Iceberg change | Flink row kind |
| --- | --- |
| Added row | `+I` |
| Deleted row | `-D` |
| Removed file row | `-D` |

## Connector Options

| Option | Required | Default | Description |
| --- | --- | --- | --- |
| `catalog-impl` | Yes | none | Iceberg catalog implementation class. |
| `warehouse` | Yes | none | Iceberg warehouse URI. |
| `table` | Yes | none | Iceberg table identifier, for example `db.events`. |
| `catalog.uri` | No | none | Catalog URI for REST/JDBC-style catalogs. |
| `catalog.region` | No | `us-east-1` | AWS client region when relevant. |
| `catalog.*` | No | none | Passed through to the Iceberg catalog after removing the `catalog.` prefix. |
| `discovery-interval` | No | `30 s` | Snapshot polling interval in unbounded mode. |
| `allowed-lateness` | No | `30 s` | Out-of-orderness bound for the source watermark. |
| `bounded` | No | `false` | If `true`, read planned snapshots and finish. |
| `scan.startup.mode` | No | `earliest` | `earliest` replays visible history; `latest` starts after the current snapshot. |
| `split-assignment-mode` | No | `ordered` | `ordered` preserves global order; `file-affinity` scales by data file path. |

## Metadata Columns

Flink SQL metadata columns expose Iceberg row lineage, changelog metadata, file
metadata, and connector-specific commit time:

```sql
CREATE TABLE events_cdc (
  id BIGINT,
  name STRING,
  row_id BIGINT METADATA FROM '_row_id',
  sequence_number BIGINT METADATA FROM '_last_updated_sequence_number',
  snapshot_id BIGINT METADATA FROM '_commit_snapshot_id',
  committed_at TIMESTAMP_LTZ(3) METADATA FROM '_commit_timestamp',
  change_type STRING METADATA FROM '_change_type',
  file_path STRING METADATA FROM '_file',
  row_position BIGINT METADATA FROM '_pos',
  spec_id INT METADATA FROM '_spec_id'
) WITH (
  'connector' = 'iceberg-cdc',
  ...
);
```

`_commit_timestamp` is useful when the operational time of a physical Iceberg
delete matters. Physical delete files identify rows to remove; they do not carry
a new business-event payload. If a temporal join must use business effective
time for deletes, model deletes as explicit tombstone/change-event rows with
their own event-time field.

## Temporal Join Example

The repository includes an end-to-end Flink SQL temporal join example under
`examples/flink-sql/temporal-join-cdc-e2e`.

The executable test writes two Iceberg source tables directly through Iceberg
APIs, then runs one unbounded Flink SQL `SELECT` over `iceberg-cdc`:

```bash
./gradlew test --tests dev.henneberger.it.SqlTemporalJoinCdcE2EIT --rerun-tasks
```

The example intentionally uses row payload event time:

```sql
LEFT JOIN customer_events_cdc FOR SYSTEM_TIME AS OF o.order_time AS c
ON o.customer_id = c.customer_id
```

This validates the target usage pattern for stream-to-state joins while avoiding
test fragility from separate writer jobs and checkpoint-dependent sink output.

## Ordering and Parallelism

`split-assignment-mode = 'ordered'` assigns all CDC splits to source subtask `0`.
Use this when downstream semantics require a single global order across
snapshots and files.

`split-assignment-mode = 'file-affinity'` assigns splits by data file path. This
allows more parallelism while preserving order for changes tied to the same data
file. It does not provide a total order across unrelated files.

## Build and Test

```bash
./gradlew test
./gradlew shadowJar
```

The test suite exercises the connector against real Iceberg tables, Parquet
files, Flink mini-clusters, deletion vectors, positional deletes, equality
deletes, overwrite snapshots, checkpoint recovery, savepoint restore, SQL DDL,
metadata columns, and the temporal join example.

Current test count: 32.

## Artifacts and Releases

The published Maven coordinate is:

```kotlin
implementation("dev.henneberger:iceberg-cdc:<version>")
```

Development builds use a local `0.1.0-SNAPSHOT` version. Release builds pass an
explicit version with `-PreleaseVersion=<version>` and publish signed artifacts
to Maven Central through `.github/workflows/release-central.yml`.

The release workflow requires these repository or organization secrets:

| Secret | Purpose |
| --- | --- |
| `MAVEN_CENTRAL_USERNAME` | Maven Central Portal token username |
| `MAVEN_CENTRAL_TOKEN` | Maven Central Portal token password |
| `MAVEN_GPG_PRIVATE_KEY` | ASCII-armored private key for artifact signing |
| `MAVEN_GPG_PASSPHRASE` | Passphrase for the signing key |

## Architecture

The connector is implemented as a FLIP-27 source:
`Source<RowData, IcebergCdcSplit, IcebergCdcEnumState>`. The Flink SQL layer is
a `ScanTableSource` wrapper that translates the resolved SQL schema, projection,
and metadata columns into the Iceberg read schema used by the source.

```text
Iceberg catalog metadata
        |
        v
IcebergCdcEnumerator
  - refresh table metadata
  - walk snapshot lineage after the checkpointed bookmark
  - classify append, row-delete, and overwrite/remove-file changes
  - produce one IcebergCdcSplit per data-file/change operation
        |
        v
IcebergCdcReader / IcebergCdcSplitReader
  - read Parquet data files through Iceberg/Flink readers
  - apply deletion vectors, positional deletes, and equality deletes
  - synthesize metadata columns
  - emit RowData with Flink RowKind
        |
        v
Flink changelog stream
```

The enumerator is the only component that reasons about Iceberg snapshot
lineage. On startup it either replays visible history (`scan.startup.mode =
'earliest'`) or bookmarks the current snapshot (`latest`). On each discovery
cycle it refreshes table metadata, validates that the checkpointed bookmark is
still in the visible lineage, and plans snapshots in parent-child order. If
history required for a correct continuation has been expired, the source fails
instead of silently replaying or skipping data.

`IcebergCdcSplit` is the unit of work assigned to readers. A split represents a
single data file under a single changelog operation, plus the delete files and
snapshot metadata needed to interpret it:

```text
(snapshot id, change ordinal, operation, data file, delete refs,
 data sequence number, first row id, commit timestamp, partition spec id)
```

For added data files, the reader emits surviving rows as `INSERT`. For
positional deletes and deletion vectors, the reader reconstructs Iceberg delete
files and loads a `PositionDeleteIndex`. For equality deletes, it reads the
equality-key columns even if projection pushdown removed them from the SQL
output, builds an Iceberg `StructLikeSet`, and then projects back to the
requested row shape. Equality delete sets are cached with a bounded LRU cache to
avoid reloading shared delete files across splits.

Overwrite and remove-file snapshots are treated as retractions of rows that were
live immediately before the remove operation. The planner maintains a parent
snapshot file index so remove-file CDC is based on the table state before the
snapshot being planned, not on the already-mutated current table state.

Ordering is explicit. In `ordered` mode, every split is assigned to source
subtask `0`; this gives a single global CDC order across snapshots and change
ordinals. In `file-affinity` mode, splits are assigned by data file path; this
improves parallelism and preserves per-file ordering, but it does not provide a
global order across unrelated files.

Checkpointing has two layers. The enumerator checkpoints
`lastConsumedSnapshotId`, which is the durable Iceberg snapshot bookmark. Each
reader also checkpoints split-local row progress, so recovery can resume inside
a large data file without re-emitting rows that already crossed a checkpoint.

Time semantics are separated from changelog correctness. The source attaches
the Iceberg snapshot commit timestamp to emitted records and exposes it in SQL
as `_commit_timestamp`. SQL jobs can instead define watermarks over physical row
fields such as `event_time` or `change_time`. For temporal joins, that choice is
semantic: physical Iceberg delete files do not carry a new business-time payload,
so business-time deletes should be modeled as explicit tombstone/change-event
rows. `_commit_timestamp` represents operational commit time.
