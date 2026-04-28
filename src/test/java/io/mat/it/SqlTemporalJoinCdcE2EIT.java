package io.mat.it;

import org.apache.flink.core.execution.JobClient;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.types.Row;
import org.apache.flink.types.RowKind;
import org.apache.flink.util.CloseableIterator;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.math.BigDecimal;
import java.nio.file.Path;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Code-generated Iceberg sources plus one unbounded Flink SQL temporal join.
 *
 * <p>The source tables are written directly through Iceberg APIs so this test
 * exercises the CDC SQL connector and temporal join without depending on
 * additional Flink SQL writer jobs or Iceberg sink checkpoint visibility.
 */
class SqlTemporalJoinCdcE2EIT {

    private static final TableIdentifier ORDERS_ID = TableIdentifier.of("e2e", "orders");
    private static final TableIdentifier CUSTOMERS_ID = TableIdentifier.of("e2e", "customer_events");

    private static final Schema ORDERS_SCHEMA = new Schema(
            Types.NestedField.required(1, "order_id", Types.LongType.get()),
            Types.NestedField.required(2, "customer_id", Types.LongType.get()),
            Types.NestedField.required(3, "amount", Types.DecimalType.of(10, 2)),
            Types.NestedField.optional(4, "order_note", Types.StringType.get()),
            Types.NestedField.required(5, "order_time", Types.TimestampType.withoutZone()));

    private static final Schema CUSTOMERS_SCHEMA = new Schema(
            Types.NestedField.required(1, "customer_id", Types.LongType.get()),
            Types.NestedField.optional(2, "customer_name", Types.StringType.get()),
            Types.NestedField.optional(3, "tier", Types.StringType.get()),
            Types.NestedField.required(4, "is_deleted", Types.BooleanType.get()),
            Types.NestedField.required(5, "change_time", Types.TimestampType.withoutZone()));

    @TempDir
    Path tempDir;

    @Test
    void unboundedSqlTemporalJoinUsesRowEventTimeFromIcebergSources() throws Exception {
        String warehouse = tempDir.resolve("warehouse").toUri().toString();

        try (IcebergTestHarness harness = new IcebergTestHarness(warehouse)) {
            Table orders = harness.createTable(ORDERS_ID, ORDERS_SCHEMA, PartitionSpec.unpartitioned(), "2");
            Table customers = harness.createTable(CUSTOMERS_ID, CUSTOMERS_SCHEMA, PartitionSpec.unpartitioned(), "2");

            TableResult result = startTemporalJoinSelect(warehouse);
            JobClient job = result.getJobClient().orElseThrow();

            ExecutorService executor = Executors.newFixedThreadPool(2);
            CompletableFuture<Void> generator = CompletableFuture.runAsync(
                    () -> generateSourceSnapshots(harness, orders, customers), executor);

            List<EnrichedOrder> actual;
            try (CloseableIterator<Row> rows = result.collect()) {
                CompletableFuture<List<EnrichedOrder>> collector = CompletableFuture.supplyAsync(
                        () -> collectJoinedRows(rows, 3), executor);
                actual = collector.get(45, TimeUnit.SECONDS);
                generator.get(5, TimeUnit.SECONDS);
            } finally {
                job.cancel().get(30, TimeUnit.SECONDS);
                executor.shutdownNow();
                executor.awaitTermination(5, TimeUnit.SECONDS);
            }

            actual.sort(Comparator.comparingLong(EnrichedOrder::orderId));
            assertEquals(List.of(
                    new EnrichedOrder(1001L, 1L, new BigDecimal("12.50"),
                            "alice", "gold", true),
                    new EnrichedOrder(1002L, 2L, new BigDecimal("99.00"),
                            "bob", "silver", true),
                    new EnrichedOrder(1003L, 1L, new BigDecimal("41.25"),
                            null, null, false)
            ), actual);
        }
    }

    private static TableResult startTemporalJoinSelect(String warehouse) {
        StreamTableEnvironment tEnv = tableEnv("mat-e2e-codegen-cdc-temporal-select");
        tEnv.executeSql(""
                + "CREATE TEMPORARY TABLE orders_cdc ("
                + "  order_id BIGINT,"
                + "  customer_id BIGINT,"
                + "  amount DECIMAL(10, 2),"
                + "  order_note STRING,"
                + "  order_time TIMESTAMP(3),"
                + "  WATERMARK FOR order_time AS order_time - INTERVAL '0' SECOND"
                + ") WITH ("
                + "  'connector' = 'iceberg-cdc',"
                + "  'catalog-impl' = 'org.apache.iceberg.hadoop.HadoopCatalog',"
                + "  'warehouse' = '" + warehouse + "',"
                + "  'table' = 'e2e.orders',"
                + "  'discovery-interval' = '200 ms',"
                + "  'scan.startup.mode' = 'latest',"
                + "  'bounded' = 'false',"
                + "  'split-assignment-mode' = 'ordered'"
                + ")");

        tEnv.executeSql(""
                + "CREATE TEMPORARY TABLE customer_events_cdc ("
                + "  customer_id BIGINT NOT NULL,"
                + "  customer_name STRING,"
                + "  tier STRING,"
                + "  is_deleted BOOLEAN,"
                + "  change_time TIMESTAMP(3),"
                + "  WATERMARK FOR change_time AS change_time - INTERVAL '0' SECOND,"
                + "  PRIMARY KEY (customer_id) NOT ENFORCED"
                + ") WITH ("
                + "  'connector' = 'iceberg-cdc',"
                + "  'catalog-impl' = 'org.apache.iceberg.hadoop.HadoopCatalog',"
                + "  'warehouse' = '" + warehouse + "',"
                + "  'table' = 'e2e.customer_events',"
                + "  'discovery-interval' = '200 ms',"
                + "  'scan.startup.mode' = 'latest',"
                + "  'bounded' = 'false',"
                + "  'split-assignment-mode' = 'ordered'"
                + ")");

        return tEnv.executeSql(""
                + "SELECT "
                + "  o.order_id,"
                + "  o.customer_id,"
                + "  o.amount,"
                + "  CASE WHEN COALESCE(c.is_deleted, TRUE) = FALSE THEN c.customer_name ELSE CAST(NULL AS STRING) END,"
                + "  CASE WHEN COALESCE(c.is_deleted, TRUE) = FALSE THEN c.tier ELSE CAST(NULL AS STRING) END,"
                + "  COALESCE(c.is_deleted = FALSE, FALSE) AS matched_customer "
                + "FROM orders_cdc AS o "
                + "LEFT JOIN customer_events_cdc FOR SYSTEM_TIME AS OF o.order_time AS c "
                + "ON o.customer_id = c.customer_id "
                + "WHERE o.order_id > 0");
    }

    private static StreamTableEnvironment tableEnv(String pipelineName) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        EnvironmentSettings settings = EnvironmentSettings.newInstance().inStreamingMode().build();
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env, settings);
        tEnv.getConfig().getConfiguration().setString("pipeline.name", pipelineName);
        tEnv.getConfig().getConfiguration().setString("execution.runtime-mode", "streaming");
        return tEnv;
    }

    private static void generateSourceSnapshots(
            IcebergTestHarness harness, Table orders, Table customers) {
        try {
            sleep(500);
            harness.appendRecords(customers, List.of(customer(
                    1L, "alice", "gold", false, "2026-01-01T00:00:00")));
            sleep(500);
            harness.appendRecords(orders, List.of(order(
                    1001L, 1L, "12.50", "matches alice before delete", "2026-01-01T00:00:15")));
            sleep(500);
            harness.appendRecords(customers, List.of(customer(
                    2L, "bob", "silver", false, "2026-01-01T00:00:30")));
            sleep(500);
            harness.appendRecords(orders, List.of(order(
                    1002L, 2L, "99.00", "matches bob after insert", "2026-01-01T00:00:45")));
            sleep(500);
            harness.appendRecords(customers, List.of(customer(
                    1L, null, null, true, "2026-01-01T00:01:00")));
            sleep(500);
            harness.appendRecords(orders, List.of(order(
                    1003L, 1L, "41.25", "alice was deleted before this order", "2026-01-01T00:01:15")));
            sleep(500);
            harness.appendRecords(customers, List.of(customer(
                    3L, "carol", "bronze", false, "2026-01-01T00:01:30")));
            sleep(500);
            harness.appendRecords(orders, List.of(order(
                    -1L, -1L, "0.00", "watermark advance", "2026-01-01T00:01:45")));
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private static List<EnrichedOrder> collectJoinedRows(CloseableIterator<Row> rows, int expectedRows) {
        List<EnrichedOrder> out = new ArrayList<>();
        while (out.size() < expectedRows && rows.hasNext()) {
            Row row = rows.next();
            if (row.getKind() != RowKind.INSERT && row.getKind() != RowKind.UPDATE_AFTER) {
                continue;
            }
            out.add(new EnrichedOrder(
                    ((Number) row.getField(0)).longValue(),
                    ((Number) row.getField(1)).longValue(),
                    ((BigDecimal) row.getField(2)).setScale(2),
                    (String) row.getField(3),
                    (String) row.getField(4),
                    (Boolean) row.getField(5)));
        }
        return out;
    }

    private static Record order(
            long orderId, long customerId, String amount, String note, String orderTime) {
        GenericRecord r = GenericRecord.create(ORDERS_SCHEMA);
        r.setField("order_id", orderId);
        r.setField("customer_id", customerId);
        r.setField("amount", new BigDecimal(amount));
        r.setField("order_note", note);
        r.setField("order_time", LocalDateTime.parse(orderTime));
        return r;
    }

    private static Record customer(
            long customerId, String name, String tier, boolean deleted, String changeTime) {
        GenericRecord r = GenericRecord.create(CUSTOMERS_SCHEMA);
        r.setField("customer_id", customerId);
        r.setField("customer_name", name);
        r.setField("tier", tier);
        r.setField("is_deleted", deleted);
        r.setField("change_time", LocalDateTime.parse(changeTime));
        return r;
    }

    private static void sleep(long millis) {
        try {
            Thread.sleep(millis);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException(e);
        }
    }

    private record EnrichedOrder(
            long orderId,
            long customerId,
            BigDecimal amount,
            String customerName,
            String tier,
            boolean matchedCustomer) {}
}
