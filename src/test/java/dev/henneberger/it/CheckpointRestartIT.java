package dev.henneberger.it;

import dev.henneberger.source.IcebergCdcEnumState;
import dev.henneberger.source.IcebergCdcEnumStateSerializer;
import dev.henneberger.source.IcebergCdcSource;
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

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Verifies that checkpointed enumerator state persists across job restarts.
 *
 * <ol>
 *   <li>Run #1 reads snapshots S1, S2.</li>
 *   <li>State is serialized and deserialized (simulating a savepoint).</li>
 *   <li>More snapshots S3, S4 are appended to the table.</li>
 *   <li>Run #2 starts from the restored bookmark and reads ONLY S3, S4 — no replay.</li>
 * </ol>
 */
class CheckpointRestartIT {

    @TempDir
    Path tempDir;

    private static final Schema SCHEMA = new Schema(
            Types.NestedField.required(1, "id", Types.LongType.get()),
            Types.NestedField.required(2, "name", Types.StringType.get()));

    @Test
    void enumStateSerializerRoundTripPreservesBookmark() throws Exception {
        IcebergCdcEnumState state = new IcebergCdcEnumState(123456789L);
        byte[] bytes = IcebergCdcEnumStateSerializer.INSTANCE.serialize(state);
        IcebergCdcEnumState restored = IcebergCdcEnumStateSerializer.INSTANCE
                .deserialize(IcebergCdcEnumStateSerializer.INSTANCE.getVersion(), bytes);
        assertEquals(123456789L, restored.lastConsumedSnapshotId);
    }

    @Test
    void enumStateSerializerRestoresVersion3Savepoints() throws Exception {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        try (DataOutputStream out = new DataOutputStream(baos)) {
            out.writeLong(123456789L);
            out.writeInt(0);
        }

        IcebergCdcEnumState restored = IcebergCdcEnumStateSerializer.INSTANCE
                .deserialize(3, baos.toByteArray());
        assertEquals(123456789L, restored.lastConsumedSnapshotId);
        assertTrue(restored.startupInitialized,
                "pre-startup-mode savepoints should restore as already initialized");
    }

    @Test
    void bookmarkDriftFailsLoudlyInsteadOfReplaying() throws Exception {
        // Regression: previously, if the stored snapshot id was no longer in history
        // (snapshot expired / table rolled back), the enumerator would silently walk
        // back to the root and replay everything. Now it must fail loud.
        String warehouse = tempDir.toUri().toString();
        TableIdentifier id = TableIdentifier.of("ckpt", "drift");

        try (IcebergTestHarness harness = new IcebergTestHarness(warehouse)) {
            Table table = harness.createTable(id, SCHEMA, PartitionSpec.unpartitioned(), "3");
            appendSnapshot(table, List.of(rec(1L, "a")));

            // Restart from a bookmark that does NOT exist in the table's history.
            IcebergCdcEnumState ghostState = new IcebergCdcEnumState(99999999999999L,
                    new java.util.ArrayList<>());

            try {
                readBoundedFromBookmark(warehouse, id, ghostState);
                throw new AssertionError("expected drift detection to throw");
            } catch (Exception e) {
                // Walk down to the root cause.
                Throwable cause = e;
                while (cause.getCause() != null) cause = cause.getCause();
                assertTrue(cause.getMessage() != null
                                && cause.getMessage().contains("no longer in the table's snapshot history"),
                        "expected drift error, got: " + cause);
            }
        }
    }

    @Test
    void run1ReadsEarlySnapshotsRun2ResumesFromBookmark() throws Exception {
        String warehouse = tempDir.toUri().toString();
        TableIdentifier id = TableIdentifier.of("ckpt", "events");

        try (IcebergTestHarness harness = new IcebergTestHarness(warehouse)) {
            Table table = harness.createTable(id, SCHEMA, PartitionSpec.unpartitioned(), "3");

            // Snapshots S1, S2 — two single-row inserts.
            appendSnapshot(table, List.of(rec(1L, "a")));
            appendSnapshot(table, List.of(rec(2L, "b")));
            table.refresh();
            long s2Id = table.currentSnapshot().snapshotId();

            // Run #1 — bounded read of [start, S2]. Capture emitted rows.
            List<EmittedRow> emittedRun1 = readBounded(warehouse, id);
            assertEquals(2, emittedRun1.size(), "run 1 reads S1 + S2: " + emittedRun1);

            // Simulate savepoint+restore by serializing and deserializing the bookmark.
            IcebergCdcEnumState saved = new IcebergCdcEnumState(s2Id);
            byte[] bytes = IcebergCdcEnumStateSerializer.INSTANCE.serialize(saved);
            IcebergCdcEnumState restored = IcebergCdcEnumStateSerializer.INSTANCE
                    .deserialize(IcebergCdcEnumStateSerializer.INSTANCE.getVersion(), bytes);
            assertEquals(s2Id, restored.lastConsumedSnapshotId);

            // Two more snapshots after the savepoint.
            appendSnapshot(harness.catalog().loadTable(id), List.of(rec(3L, "c")));
            appendSnapshot(harness.catalog().loadTable(id), List.of(rec(4L, "d")));

            // Run #2 — start from the restored bookmark. Should ONLY see S3 + S4.
            List<EmittedRow> emittedRun2 = readBoundedFromBookmark(warehouse, id, restored);
            assertEquals(2, emittedRun2.size(), "run 2 sees only post-bookmark snapshots: " + emittedRun2);
            assertEquals(3L, emittedRun2.get(0).id);
            assertEquals(4L, emittedRun2.get(1).id);
            // Make sure no replay of S1 / S2.
            for (EmittedRow r : emittedRun2) {
                if (r.id == 1L || r.id == 2L) {
                    throw new AssertionError("run 2 wrongly replayed snapshot bookmark " + r);
                }
            }
        }
    }

    private List<EmittedRow> readBounded(String warehouse, TableIdentifier id) throws Exception {
        return readBoundedFromBookmark(warehouse, id, IcebergCdcEnumState.empty());
    }

    private List<EmittedRow> readBoundedFromBookmark(String warehouse, TableIdentifier id,
                                                     IcebergCdcEnumState restoredState) throws Exception {
        IcebergCdcSource source = new IcebergCdcSource(
                new SerializableTableSupplier(warehouse, id.namespace().toString(), id.name()),
                SCHEMA, Duration.ofMillis(500), /* bounded = */ true, restoredState);

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        DataStream<RowData> stream = env.fromSource(source,
                WatermarkStrategy.noWatermarks(), "iceberg-cdc-source");

        List<EmittedRow> out = new ArrayList<>();
        try (CloseableIterator<RowData> it = stream.executeAndCollect("checkpoint-restart-test")) {
            while (it.hasNext()) {
                RowData row = it.next();
                out.add(new EmittedRow(row.getRowKind(), row.getLong(0),
                        row.getString(1) == null ? null : row.getString(1).toString()));
            }
        }
        return out;
    }

    private static Snapshot appendSnapshot(Table table, List<Record> records) throws Exception {
        DataFile file = writeRecords(table, records);
        table.newAppend().appendFile(file).commit();
        table.refresh();
        return table.currentSnapshot();
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

    private record EmittedRow(RowKind kind, long id, String value) {}

    private static final class SerializableTableSupplier
            implements dev.henneberger.source.IcebergCdcEnumerator.TableSupplier {
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
