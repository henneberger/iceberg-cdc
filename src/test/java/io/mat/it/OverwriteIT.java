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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Iceberg overwrite operations: a snapshot can REMOVE existing data files and ADD new ones
 * atomically. The CDC reader must emit -D for the rows in the removed files and +I for
 * the rows in the added files, in the same change ordinal.
 *
 * <p>This is the path Spark/Flink upserts take when using "rewrite-style" updates instead
 * of equality-delete + new-row pairs. It's also what {@code MERGE INTO} produces for
 * row-level updates in some engines.
 */
class OverwriteIT {

    @TempDir
    Path tempDir;

    private static final Schema SCHEMA = new Schema(
            Types.NestedField.required(1, "id", Types.LongType.get()),
            Types.NestedField.required(2, "name", Types.StringType.get()));

    @Test
    void overwriteEmitsDeleteForRemovedFilesAndInsertForAddedFiles() throws Exception {
        String warehouse = tempDir.toUri().toString();
        TableIdentifier id = TableIdentifier.of("ovw", "events");

        try (IcebergTestHarness harness = new IcebergTestHarness(warehouse)) {
            Table table = harness.createTable(id, SCHEMA, PartitionSpec.unpartitioned(), "3");

            // Snapshot 1: insert 3 rows.
            DataFile file1 = writeRecords(table, List.of(
                    rec(1L, "old-1"), rec(2L, "old-2"), rec(3L, "old-3")));
            table.newAppend().appendFile(file1).commit();
            table.refresh();

            // Snapshot 2: overwrite — remove file1, replace with file2 containing 2 different rows.
            // This is the "row-level update" pattern: drop old version, write new one.
            DataFile file2 = writeRecords(table, List.of(
                    rec(2L, "new-2"), rec(3L, "new-3")));
            table.newOverwrite().deleteFile(file1).addFile(file2).commit();
            table.refresh();

            String wh = warehouse;
            String tableNs = id.namespace().toString();
            String tableName = id.name();
            IcebergCdcSource source = new IcebergCdcSource(
                    new SerializableTableSupplier(wh, tableNs, tableName),
                    SCHEMA, Duration.ofSeconds(1), true);

            StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
            env.setParallelism(1);
            DataStream<RowData> stream = env.fromSource(source,
                    WatermarkStrategy.noWatermarks(), "iceberg-cdc-source");

            List<EmittedRow> emitted = new ArrayList<>();
            try (CloseableIterator<RowData> it = stream.executeAndCollect("overwrite")) {
                while (it.hasNext()) {
                    RowData row = it.next();
                    emitted.add(new EmittedRow(row.getRowKind(), row.getLong(0),
                            row.getString(1).toString()));
                }
            }
            System.out.println("[overwrite] emitted: " + emitted);

            // Snapshot 1: 3× +I. Snapshot 2: 3× -D (file1 removed) + 2× +I (file2 added).
            assertEquals(8, emitted.size(), "3 inserts + 3 deletes + 2 inserts: " + emitted);

            long inserts = emitted.stream().filter(r -> r.kind == RowKind.INSERT).count();
            long deletes = emitted.stream().filter(r -> r.kind == RowKind.DELETE).count();
            assertEquals(5, inserts, "5 INSERTs (3 initial + 2 new)");
            assertEquals(3, deletes, "3 DELETEs (file1 had 3 rows; whole file removed)");

            // Net effect: ids 2 and 3 were "updated" (their old values deleted + new values
            // inserted). Id 1 was outright deleted. The downstream SQL planner pairs -D/+I
            // by primary key into UPDATE_BEFORE/UPDATE_AFTER.
            Map<Long, List<RowKind>> kindsById = new HashMap<>();
            for (EmittedRow r : emitted) {
                kindsById.computeIfAbsent(r.id, k -> new ArrayList<>()).add(r.kind);
            }
            // id=1: +I, then -D → eventual delete
            assertEquals(List.of(RowKind.INSERT, RowKind.DELETE), kindsById.get(1L));
            // id=2: +I (old), -D (old), +I (new) → eventual update with new value
            assertTrue(kindsById.get(2L).contains(RowKind.INSERT)
                    && kindsById.get(2L).contains(RowKind.DELETE));
            assertEquals(2, kindsById.get(2L).stream().filter(k -> k == RowKind.INSERT).count(),
                    "id=2 has two INSERTs: old then new value");
        }
    }

    @Test
    void removedFileDeletesOnlyRowsLiveBeforeRemoval() throws Exception {
        String warehouse = tempDir.toUri().toString();
        TableIdentifier id = TableIdentifier.of("ovw", "events_with_prior_delete");

        try (IcebergTestHarness harness = new IcebergTestHarness(warehouse)) {
            Table table = harness.createTable(id, SCHEMA, PartitionSpec.unpartitioned(), "3");

            DataFile file1 = writeRecords(table, List.of(
                    rec(1L, "old-1"), rec(2L, "old-2"), rec(3L, "old-3")));
            table.newAppend().appendFile(file1).commit();
            table.refresh();

            DeleteFile dv = writeDvFor(table, file1.path().toString(), new long[] { 1L });
            table.newRowDelta().addDeletes(dv).commit();
            table.refresh();

            table.newOverwrite().deleteFile(file1).commit();
            table.refresh();

            IcebergCdcSource source = new IcebergCdcSource(
                    new SerializableTableSupplier(warehouse, id.namespace().toString(), id.name()),
                    SCHEMA, Duration.ofSeconds(1), true);

            StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
            env.setParallelism(1);
            DataStream<RowData> stream = env.fromSource(source,
                    WatermarkStrategy.noWatermarks(), "overwrite-live-rows");

            List<EmittedRow> emitted = new ArrayList<>();
            try (CloseableIterator<RowData> it = stream.executeAndCollect("overwrite-live-rows")) {
                while (it.hasNext()) {
                    RowData row = it.next();
                    emitted.add(new EmittedRow(row.getRowKind(), row.getLong(0),
                            row.getString(1).toString()));
                }
            }

            List<Long> deletedIds = emitted.stream()
                    .filter(r -> r.kind == RowKind.DELETE)
                    .map(r -> r.id)
                    .sorted()
                    .toList();
            assertEquals(List.of(1L, 2L, 3L), deletedIds,
                    "id=2 must be deleted once by the DV, not again by file removal: " + emitted);
            assertEquals(1, emitted.stream()
                    .filter(r -> r.kind == RowKind.DELETE && r.id == 2L)
                    .count());
        }
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

    private static DeleteFile writeDvFor(Table table, String dataFilePath, long[] positions) throws Exception {
        OutputFileFactory fileFactory = OutputFileFactory.builderFor(table, 0, 0)
                .format(FileFormat.PUFFIN)
                .build();
        BaseDVFileWriter dvWriter = new BaseDVFileWriter(fileFactory, p -> null);
        try (dvWriter) {
            for (long pos : positions) {
                dvWriter.delete(dataFilePath, pos, table.spec(), null);
            }
        }
        return dvWriter.result().deleteFiles().get(0);
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
