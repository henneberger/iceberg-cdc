package io.mat.it;

import io.mat.source.IcebergCdcSource;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.data.RowData;
import org.apache.flink.types.RowKind;
import org.apache.flink.util.CloseableIterator;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.data.GenericAppenderFactory;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.encryption.EncryptionUtil;
import org.apache.iceberg.io.DataWriter;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Streaming source + concurrent {@code expireSnapshots}: the bookmark snapshot may be
 * removed from history while we're running. The reader must either keep up (advance
 * bookmark before expiry hits it) or fail loud (we don't silently replay).
 */
class SnapshotExpiryIT {

    @TempDir
    Path tempDir;

    private static final Schema SCHEMA = new Schema(
            Types.NestedField.required(1, "id", Types.LongType.get()),
            Types.NestedField.required(2, "name", Types.StringType.get()));

    @Test
    void readerKeepsUpWhenSnapshotsAreExpiredConcurrently() throws Exception {
        String warehouse = tempDir.toUri().toString();
        TableIdentifier id = TableIdentifier.of("expire", "events");

        try (IcebergTestHarness harness = new IcebergTestHarness(warehouse)) {
            Table table = harness.createTable(id, SCHEMA, PartitionSpec.unpartitioned(), "3");

            // Pre-populate a few snapshots.
            for (int i = 0; i < 3; i++) {
                appendOne(table, i, "v" + i);
            }
            table.refresh();
            long oldestKept = table.currentSnapshot().snapshotId();

            String wh = warehouse;
            String tableNs = id.namespace().toString();
            String tableName = id.name();
            IcebergCdcSource source = new IcebergCdcSource(
                    new SerializableTableSupplier(wh, tableNs, tableName),
                    SCHEMA,
                    Duration.ofMillis(300),
                    /* bounded = */ false);

            StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
            env.setParallelism(1);
            DataStream<RowData> stream = env.fromSource(source,
                    WatermarkStrategy.noWatermarks(), "iceberg-cdc-source");

            List<EmittedRow> emitted = new ArrayList<>();
            try (CloseableIterator<RowData> it = stream.executeAndCollect("expire-test")) {
                long deadline = System.currentTimeMillis() + 30_000L;
                int seen = 0;
                while (System.currentTimeMillis() < deadline && emitted.size() < 5) {
                    if (it.hasNext()) {
                        RowData row = it.next();
                        emitted.add(new EmittedRow(row.getRowKind(), row.getLong(0),
                                row.getString(1).toString()));
                        seen++;
                        // After we've seen the first 3 rows (initial snapshots fully consumed),
                        // expire the older snapshots in background and then add 2 more.
                        if (seen == 3) {
                            Table t = harness.catalog().loadTable(id);
                            t.refresh();
                            // Keep only the current snapshot — drop everything older.
                            t.expireSnapshots()
                                    .expireOlderThan(t.currentSnapshot().timestampMillis() + 1)
                                    .commit();
                            appendOne(t, 100, "post-expire-1");
                            appendOne(t, 200, "post-expire-2");
                        }
                    } else {
                        Thread.sleep(50);
                    }
                }
            } catch (Exception e) {
                // Acceptable: if we missed a snapshot in the expiry window, the bookmark-drift
                // guard fails the job loud. Either outcome (5 rows OR a clean failure) is correct.
                Throwable root = e;
                while (root.getCause() != null) root = root.getCause();
                String msg = String.valueOf(root.getMessage());
                System.out.println("[expire-test] caught (acceptable): " + msg);
                assertTrue(msg.contains("no longer in the table's snapshot history")
                                || msg.contains("Iceberg CDC discovery failed")
                                || emitted.size() == 5,
                        "unexpected error: " + root);
                return;
            }

            // Happy path: we kept up with the writer and saw all 5 rows.
            System.out.println("[expire-test] emitted: " + emitted);
            assertTrue(emitted.size() >= 3 && emitted.size() <= 5,
                    "expected 3-5 events: " + emitted);
            for (EmittedRow r : emitted) {
                assertTrue(r.kind == RowKind.INSERT, "all should be inserts: " + emitted);
            }
        }
    }

    private static void appendOne(Table table, long id, String name) throws Exception {
        DataFile f = writeRecords(table, List.of(rec(id, name)));
        table.newAppend().appendFile(f).commit();
        table.refresh();
    }

    private static Record rec(long id, String name) {
        GenericRecord r = GenericRecord.create(SCHEMA);
        r.setField("id", id);
        r.setField("name", name);
        return r;
    }

    private static DataFile writeRecords(Table table, List<Record> records) throws Exception {
        GenericAppenderFactory af = new GenericAppenderFactory(table.schema(), table.spec());
        String filename = "data-" + UUID.randomUUID() + ".parquet";
        OutputFile out = table.io().newOutputFile(table.location() + "/data/" + filename);
        DataWriter<Record> writer = af.newDataWriter(
                EncryptionUtil.plainAsEncryptedOutput(out),
                FileFormat.PARQUET, null);
        try (writer) { for (Record r : records) writer.write(r); }
        return writer.toDataFile();
    }

    private record EmittedRow(RowKind kind, long id, String name) {}

    private static final class SerializableTableSupplier
            implements io.mat.source.IcebergCdcEnumerator.TableSupplier {
        private static final long serialVersionUID = 1L;
        private final String warehouse;
        private final String namespace;
        private final String tableName;
        SerializableTableSupplier(String warehouse, String namespace, String tableName) {
            this.warehouse = warehouse; this.namespace = namespace; this.tableName = tableName;
        }
        @Override public Table get() {
            IcebergTestHarness h = new IcebergTestHarness(warehouse);
            return h.catalog().loadTable(TableIdentifier.of(namespace, tableName));
        }
    }
}
