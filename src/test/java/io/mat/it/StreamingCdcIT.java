package io.mat.it;

import io.mat.source.IcebergCdcSource;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.data.RowData;
import org.apache.flink.types.RowKind;
import org.apache.flink.util.CloseableIterator;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.data.GenericAppenderFactory;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.deletes.BaseDVFileWriter;
import org.apache.iceberg.encryption.EncryptionUtil;
import org.apache.iceberg.io.DataWriter;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.io.OutputFileFactory;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Unbounded streaming mode: the reader keeps polling for new snapshots and
 * picks them up as they're committed. Verifies that:
 * <ul>
 *   <li>The source starts emitting events from existing snapshots.</li>
 *   <li>Events committed AFTER the job started are picked up by the next
 *       discovery tick.</li>
 *   <li>Both INSERT (added data files) and DELETE (DV) events flow.</li>
 * </ul>
 */
class StreamingCdcIT {

    @TempDir
    Path tempDir;

    private static final Schema SCHEMA = new Schema(
            Types.NestedField.required(1, "id", Types.LongType.get()),
            Types.NestedField.required(2, "name", Types.StringType.get()));

    @Test
    void streamingReaderPicksUpSnapshotsCommittedAfterJobStart() throws Exception {
        String warehouse = tempDir.toUri().toString();
        TableIdentifier id = TableIdentifier.of("stream", "events");

        try (IcebergTestHarness harness = new IcebergTestHarness(warehouse)) {
            Table table = harness.createTable(id, SCHEMA, PartitionSpec.unpartitioned(), "3");

            // Initial snapshot.
            DataFile file1 = writeRecords(table, List.of(rec(1L, "a"), rec(2L, "b")));
            table.newAppend().appendFile(file1).commit();
            table.refresh();

            String wh = warehouse;
            String tableNs = id.namespace().toString();
            String tableName = id.name();

            IcebergCdcSource source = new IcebergCdcSource(
                    new SerializableTableSupplier(wh, tableNs, tableName),
                    SCHEMA,
                    Duration.ofMillis(500),
                    /* bounded = */ false);

            StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
            env.setParallelism(1);
            DataStream<RowData> stream = env.fromSource(source,
                    WatermarkStrategy.noWatermarks(), "iceberg-cdc-source");

            // Schedule a second snapshot to be committed after the job starts.
            ScheduledExecutorService writer = Executors.newSingleThreadScheduledExecutor();
            writer.schedule(() -> {
                try {
                    Table t = harness.catalog().loadTable(id);
                    t.refresh();
                    DataFile file2 = writeRecords(t, List.of(rec(3L, "c"), rec(4L, "d")));
                    t.newAppend().appendFile(file2).commit();
                    t.refresh();
                    // Now also delete id=1 via DV.
                    DeleteFile dv = writeDvFor(t, file1.path().toString(), new long[] { 0L });
                    t.newRowDelta().addDeletes(dv).commit();
                    t.refresh();
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }, 1500, TimeUnit.MILLISECONDS);

            List<EmittedRow> emitted = new ArrayList<>();
            try (CloseableIterator<RowData> it = stream.executeAndCollect("streaming-cdc-test")) {
                long deadline = System.currentTimeMillis() + 30_000L;
                // We expect 5 events: +I(1), +I(2), +I(3), +I(4), -D(1).
                while (emitted.size() < 5 && System.currentTimeMillis() < deadline) {
                    if (it.hasNext()) {
                        RowData row = it.next();
                        emitted.add(new EmittedRow(row.getRowKind(), row.getLong(0),
                                row.getString(1) == null ? null : row.getString(1).toString()));
                    } else {
                        Thread.sleep(50);
                    }
                }
            } finally {
                writer.shutdown();
                writer.awaitTermination(5, TimeUnit.SECONDS);
            }
            System.out.println("[streaming-cdc] emitted: " + emitted);

            assertTrue(emitted.size() >= 5, "expected ≥5 events, got " + emitted.size() + ": " + emitted);
            long inserts = emitted.stream().filter(r -> r.kind == RowKind.INSERT).count();
            long deletes = emitted.stream().filter(r -> r.kind == RowKind.DELETE).count();
            assertEquals(4, inserts, "4 inserts: " + emitted);
            assertEquals(1, deletes, "1 delete: " + emitted);

            // The DELETE must be id=1 (the row at position 0 in file1).
            EmittedRow del = emitted.stream().filter(r -> r.kind == RowKind.DELETE)
                    .findFirst().orElseThrow();
            assertEquals(1L, del.id);
        }
    }

    // --- helpers ---

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

    private static DeleteFile writeDvFor(Table table, String dataFilePath, long[] positions) throws Exception {
        OutputFileFactory fileFactory = OutputFileFactory.builderFor(table, 0, 0)
                .format(FileFormat.PUFFIN).build();
        BaseDVFileWriter dvWriter = new BaseDVFileWriter(fileFactory, p -> null);
        try (dvWriter) {
            for (long pos : positions) dvWriter.delete(dataFilePath, pos, table.spec(), null);
        }
        return dvWriter.result().deleteFiles().get(0);
    }

    private record EmittedRow(RowKind kind, long id, String value) {}

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
