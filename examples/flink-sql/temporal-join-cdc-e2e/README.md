# Iceberg CDC Temporal Join E2E

This example is executable as `SqlTemporalJoinCdcE2EIT`.

The test writes the two Iceberg source tables directly through Iceberg APIs,
then runs one unbounded Flink SQL `SELECT` over the `iceberg-cdc` connector.
That keeps the test focused on CDC source discovery, rowtime watermarks, and
Flink's temporal join, without depending on separate SQL writer jobs or Iceberg
sink checkpoint visibility.

Source event schedule:

| event_time | source | event |
| --- | --- | --- |
| 00:00:00 | customer_events | customer 1 = alice/gold |
| 00:00:15 | orders | order 1001 for customer 1 |
| 00:00:30 | customer_events | customer 2 = bob/silver |
| 00:00:45 | orders | order 1002 for customer 2 |
| 00:01:00 | customer_events | customer 1 logical delete |
| 00:01:15 | orders | order 1003 for customer 1 |
| 00:01:30 | customer_events | watermark advance row |
| 00:01:45 | orders | filtered watermark advance row |

Expected SELECT output:

| order_id | customer_id | customer_name | tier | matched_customer |
| --- | --- | --- | --- | --- |
| 1001 | 1 | alice | gold | true |
| 1002 | 2 | bob | silver | true |
| 1003 | 1 | null | null | false |

Run it:

```bash
./gradlew test --tests dev.henneberger.it.SqlTemporalJoinCdcE2EIT --rerun-tasks
```

`01_cdc_temporal_join_select.sql` contains the SQL shape used by the test.
The JUnit harness supplies the Hadoop catalog warehouse and code-generated
Iceberg commits.

Note on event time: this example uses row payload event time (`order_time` and
`change_time`). A physical Iceberg delete file does not carry a new delete
payload, so a temporal join cannot infer a delete's business effective time from
the deleted row alone. To stay in row-event-time semantics, this example models
the delete as a logical tombstone row in `customer_events`. For physical Iceberg
row deletes, use an operation/effective-time metadata column such as
`_commit_timestamp`, or store explicit tombstone/change-event rows.
