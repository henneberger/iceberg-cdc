package io.mat.it;

import io.mat.source.IcebergCdcEnumState;
import io.mat.source.IcebergCdcSource;
import io.mat.source.IcebergCdcSplit;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.util.CloseableIterator;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.data.RowData;
import org.apache.flink.types.RowKind;
import org.apache.iceberg.AppendFiles;
import org.apache.iceberg.ChangelogOperation;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.RowDelta;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.data.GenericAppenderFactory;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.deletes.DVFileWriter;
import org.apache.iceberg.deletes.PositionDelete;
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
import java.util.Map;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Real end-to-end: build a v3 Iceberg table by hand (with deletion vectors via
 * Iceberg's {@code DVFileWriter}), run our CDC source through a Flink mini
 * cluster, assert the emitted {@code +I/-D} stream matches the expected
 * changelog.
 *
 * <p>Iceberg 1.10's {@link org.apache.iceberg.IncrementalChangelogScan} returns
 * INSERT and DELETE row-kinds (not UPDATE_BEFORE/UPDATE_AFTER). Pairing them
 * into UPDATE_* is the consumer's job — Flink SQL planners do it automatically
 * when the downstream operator declares an upsert mode.
 */
class CdcEndToEndIT {

    @TempDir
    Path tempDir;

    private static final Schema SCHEMA = new Schema(
            Types.NestedField.required(1, "id", Types.LongType.get()),
            Types.NestedField.required(2, "value", Types.StringType.get()));

    @Test
    void cdcSourceEmitsInsertsThenDeletesFromV3Table() throws Exception {
        String warehouse = tempDir.toUri().toString();
        TableIdentifier id = TableIdentifier.of("cdc", "events");

        try (IcebergTestHarness harness = new IcebergTestHarness(warehouse)) {
            // v3 table — DVs land in this format-version.
            Table table = harness.createTable(id, SCHEMA, PartitionSpec.unpartitioned(), "3");

            // Snapshot 1: insert id=1 ("a"), id=2 ("b"), id=3 ("c").
            DataFile file1 = writeRecords(table, List.of(
                    rec(1L, "a"), rec(2L, "b"), rec(3L, "c")));
            AppendFiles append = table.newAppend();
            append.appendFile(file1);
            append.commit();
            table.refresh();

            // Snapshot 2: delete id=2 (position 1 in file1) via a v3 deletion vector.
            DeleteFile dv = writeDvFor(table, file1.path().toString(), new long[] { 1L });
            RowDelta delta = table.newRowDelta();
            delta.addDeletes(dv);
            delta.commit();
            table.refresh();

            // Run the CDC source bounded.
            StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
            env.setParallelism(1);

            String wh = warehouse;
            String tableNs = id.namespace().toString();
            String tableName = id.name();
            IcebergCdcSource source = new IcebergCdcSource(
                    new SerializableTableSupplier(wh, tableNs, tableName),
                    SCHEMA,
                    Duration.ofSeconds(1),
                    /* bounded = */ true);

            WatermarkStrategy<RowData> wm = WatermarkStrategy.noWatermarks();

            DataStream<RowData> stream = env.fromSource(source, wm, "iceberg-cdc-source");

            // Execute and collect.
            List<EmittedRow> emitted = new ArrayList<>();
            try (CloseableIterator<RowData> it = stream.executeAndCollect("cdc-test")) {
                while (it.hasNext()) {
                    RowData row = it.next();
                    emitted.add(new EmittedRow(row.getRowKind(), row.getLong(0),
                            row.getString(1) == null ? null : row.getString(1).toString()));
                }
            }

            System.out.println("[cdc-test] emitted: " + emitted);

            // Expected:
            //   3x +I (id=1, id=2, id=3) from snapshot 1
            //   1x -D (id=2) from snapshot 2
            // Total = 4 events.
            assertEquals(4, emitted.size(), "expected 4 changelog events, got: " + emitted);

            long inserts = emitted.stream().filter(r -> r.kind == RowKind.INSERT).count();
            long deletes = emitted.stream().filter(r -> r.kind == RowKind.DELETE).count();
            assertEquals(3, inserts, "3 inserts");
            assertEquals(1, deletes, "1 delete");

            // The single -D must be id=2.
            EmittedRow del = emitted.stream().filter(r -> r.kind == RowKind.DELETE)
                    .findFirst().orElseThrow();
            assertEquals(2L, del.id);
            assertEquals("b", del.value);
        }
    }

    @Test
    void equalityDeleteEmitsDeleteEventsForAffectedPriorRows() throws Exception {
        String warehouse = tempDir.toUri().toString();
        TableIdentifier id = TableIdentifier.of("cdc", "eq_deletes");

        try (IcebergTestHarness harness = new IcebergTestHarness(warehouse)) {
            // v2 table — equality deletes are still produced by older Spark/Flink writers
            // even on v3 catalogs, so the reader must handle them.
            Table table = harness.createTable(id, SCHEMA, PartitionSpec.unpartitioned(), "2");

            DataFile file1 = writeRecords(table, List.of(rec(1L, "a"), rec(2L, "b"), rec(3L, "c")));
            table.newAppend().appendFile(file1).commit();
            table.refresh();

            // Snapshot 2: equality-delete by `id` of {2}.
            DeleteFile eqDel = writeEqualityDelete(table, List.of(2L));
            table.newRowDelta().addDeletes(eqDel).commit();
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
            try (CloseableIterator<RowData> it = stream.executeAndCollect("eq-delete-test")) {
                while (it.hasNext()) {
                    RowData row = it.next();
                    emitted.add(new EmittedRow(row.getRowKind(), row.getLong(0),
                            row.getString(1) == null ? null : row.getString(1).toString()));
                }
            }
            System.out.println("[cdc-test] emitted (eq delete): " + emitted);

            // Expected: 3x +I (id=1,2,3), then 1x -D (id=2).
            assertEquals(4, emitted.size(), "expected 4 events: " + emitted);
            assertEquals(3, emitted.stream().filter(r -> r.kind == RowKind.INSERT).count());
            assertEquals(1, emitted.stream().filter(r -> r.kind == RowKind.DELETE).count());
            EmittedRow del = emitted.stream().filter(r -> r.kind == RowKind.DELETE)
                    .findFirst().orElseThrow();
            assertEquals(2L, del.id, "equality delete by id=2");
            assertEquals("b", del.value);
        }
    }

    @Test
    void globalPositionDeleteEmitsDeleteEventsForAllReferencedFiles() throws Exception {
        String warehouse = tempDir.toUri().toString();
        TableIdentifier id = TableIdentifier.of("cdc", "global_pos_deletes");

        try (IcebergTestHarness harness = new IcebergTestHarness(warehouse)) {
            Table table = harness.createTable(id, SCHEMA, PartitionSpec.unpartitioned(), "2");

            DataFile file1 = writeRecords(table, List.of(rec(1L, "a"), rec(2L, "b"), rec(3L, "c")));
            DataFile file2 = writeRecords(table, List.of(rec(4L, "d"), rec(5L, "e")));
            table.newAppend().appendFile(file1).appendFile(file2).commit();
            table.refresh();

            // One position-delete file that references two data files. Iceberg cannot
            // encode that as a single referencedDataFile, so DeleteFile.referencedDataFile()
            // is null and the CDC planner must inspect candidate data files instead.
            DeleteFile posDeletes = writeGlobalPositionDelete(table, Map.of(
                    file1.path().toString(), new long[] { 1L },
                    file2.path().toString(), new long[] { 0L }));
            table.newRowDelta().addDeletes(posDeletes).commit();
            table.refresh();

            IcebergCdcSource source = new IcebergCdcSource(
                    new SerializableTableSupplier(warehouse, id.namespace().toString(), id.name()),
                    SCHEMA, Duration.ofSeconds(1), true);

            StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
            env.setParallelism(1);
            DataStream<RowData> stream = env.fromSource(source,
                    WatermarkStrategy.noWatermarks(), "global-pos-delete-test");

            List<EmittedRow> emitted = new ArrayList<>();
            try (CloseableIterator<RowData> it = stream.executeAndCollect("global-pos-delete-test")) {
                while (it.hasNext()) {
                    RowData row = it.next();
                    emitted.add(new EmittedRow(row.getRowKind(), row.getLong(0),
                            row.getString(1) == null ? null : row.getString(1).toString()));
                }
            }

            assertEquals(7, emitted.size(), "5 inserts + 2 position deletes: " + emitted);
            List<Long> deletedIds = emitted.stream()
                    .filter(r -> r.kind == RowKind.DELETE)
                    .map(r -> r.id)
                    .sorted()
                    .toList();
            assertEquals(List.of(2L, 4L), deletedIds);
        }
    }

    @Test
    void restoredSplitResumesFromCheckpointedRowPosition() throws Exception {
        String warehouse = tempDir.toUri().toString();
        TableIdentifier id = TableIdentifier.of("cdc", "resume_position");

        try (IcebergTestHarness harness = new IcebergTestHarness(warehouse)) {
            Table table = harness.createTable(id, SCHEMA, PartitionSpec.unpartitioned(), "3");
            DataFile file = writeRecords(table, List.of(
                    rec(1L, "a"), rec(2L, "b"), rec(3L, "c")));
            table.newAppend().appendFile(file).commit();
            table.refresh();

            IcebergCdcSplit resumedSplit = new IcebergCdcSplit(
                    "resume-from-row-2",
                    table.currentSnapshot().snapshotId(),
                    0,
                    ChangelogOperation.INSERT,
                    file.path().toString(),
                    file.fileSizeInBytes(),
                    2L,
                    file.fileSizeInBytes(),
                    file.dataSequenceNumber() == null ? -1L : file.dataSequenceNumber(),
                    file.firstRowId() == null ? -1L : file.firstRowId(),
                    table.currentSnapshot().timestampMillis(),
                    file.specId(),
                    List.of(),
                    false);
            IcebergCdcEnumState restored = new IcebergCdcEnumState(
                    table.currentSnapshot().snapshotId(), List.of(resumedSplit), true);

            IcebergCdcSource source = new IcebergCdcSource(
                    new SerializableTableSupplier(warehouse, id.namespace().toString(), id.name()),
                    SCHEMA, Duration.ofSeconds(1), true, restored);

            StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
            env.setParallelism(1);
            DataStream<RowData> stream = env.fromSource(source,
                    WatermarkStrategy.noWatermarks(), "resume-position-test");

            List<EmittedRow> emitted = new ArrayList<>();
            try (CloseableIterator<RowData> it = stream.executeAndCollect("resume-position-test")) {
                while (it.hasNext()) {
                    RowData row = it.next();
                    emitted.add(new EmittedRow(row.getRowKind(), row.getLong(0),
                            row.getString(1) == null ? null : row.getString(1).toString()));
                }
            }

            assertEquals(List.of(new EmittedRow(RowKind.INSERT, 3L, "c")), emitted);
        }
    }

    @Test
    void referencedPositionDeleteFindsLiveFileAfterAddSnapshotExpired() throws Exception {
        String warehouse = tempDir.toUri().toString();
        TableIdentifier id = TableIdentifier.of("cdc", "expired_add_snapshot_ref_delete");

        try (IcebergTestHarness harness = new IcebergTestHarness(warehouse)) {
            Table table = harness.createTable(id, SCHEMA, PartitionSpec.unpartitioned(), "3");
            DataFile file1 = writeRecords(table, List.of(rec(1L, "a")));
            table.newAppend().appendFile(file1).commit();
            table.refresh();
            long file1AddSnapshot = table.currentSnapshot().snapshotId();

            DataFile file2 = writeRecords(table, List.of(rec(2L, "b")));
            table.newAppend().appendFile(file2).commit();
            table.refresh();

            DeleteFile dv = writeDvFor(table, file1.path().toString(), new long[] { 0L });
            table.newRowDelta().addDeletes(dv).commit();
            table.refresh();

            table.expireSnapshots().expireSnapshotId(file1AddSnapshot).commit();
            table.refresh();

            IcebergCdcSource source = new IcebergCdcSource(
                    new SerializableTableSupplier(warehouse, id.namespace().toString(), id.name()),
                    SCHEMA, Duration.ofSeconds(1), true);

            StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
            env.setParallelism(1);
            DataStream<RowData> stream = env.fromSource(source,
                    WatermarkStrategy.noWatermarks(), "expired-add-snapshot-ref-delete");

            List<EmittedRow> emitted = new ArrayList<>();
            try (CloseableIterator<RowData> it =
                         stream.executeAndCollect("expired-add-snapshot-ref-delete")) {
                while (it.hasNext()) {
                    RowData row = it.next();
                    emitted.add(new EmittedRow(row.getRowKind(), row.getLong(0),
                            row.getString(1) == null ? null : row.getString(1).toString()));
                }
            }

            List<Long> deletedIds = emitted.stream()
                    .filter(r -> r.kind == RowKind.DELETE)
                    .map(r -> r.id)
                    .toList();
            assertEquals(List.of(1L), deletedIds, "referenced delete must not be skipped: " + emitted);
        }
    }

    @Test
    void compactionSnapshotsAreFiltered() throws Exception {
        String warehouse = tempDir.toUri().toString();
        TableIdentifier id = TableIdentifier.of("cdc", "compacted");

        try (IcebergTestHarness harness = new IcebergTestHarness(warehouse)) {
            Table table = harness.createTable(id, SCHEMA, PartitionSpec.unpartitioned(), "3");

            // Snapshot 1: insert two rows.
            DataFile file1 = writeRecords(table, List.of(rec(1L, "a"), rec(2L, "b")));
            table.newAppend().appendFile(file1).commit();
            table.refresh();

            // Snapshot 2: rewrite — replace file1 with file2 (same logical data, different file).
            // This is a maintenance/compaction op. Our enumerator must skip it.
            DataFile file2 = writeRecords(table, List.of(rec(1L, "a"), rec(2L, "b")));
            table.newRewrite().rewriteFiles(
                    java.util.Set.of(file1),
                    java.util.Set.of(file2)).commit();
            table.refresh();

            // CDC reader should ONLY see snapshot 1 (the user inserts), not snapshot 2 (the rewrite).
            StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
            env.setParallelism(1);

            String wh = warehouse;
            String tableNs = id.namespace().toString();
            String tableName = id.name();
            IcebergCdcSource source = new IcebergCdcSource(
                    new SerializableTableSupplier(wh, tableNs, tableName),
                    SCHEMA,
                    Duration.ofSeconds(1),
                    true);

            DataStream<RowData> stream = env.fromSource(source,
                    WatermarkStrategy.noWatermarks(), "iceberg-cdc-source");

            List<RowKind> kinds = new ArrayList<>();
            try (CloseableIterator<RowData> it = stream.executeAndCollect("cdc-test-compaction")) {
                while (it.hasNext()) {
                    kinds.add(it.next().getRowKind());
                }
            }

            assertEquals(2, kinds.size(),
                    "compaction snapshot must be filtered; expected only the 2 original inserts, got "
                            + kinds);
            assertTrue(kinds.stream().allMatch(k -> k == RowKind.INSERT),
                    "all surviving rows must be INSERT, got: " + kinds);
        }
    }

    // --- helpers ---

    private static Record rec(long id, String value) {
        GenericRecord r = GenericRecord.create(SCHEMA);
        r.setField("id", id);
        r.setField("value", value);
        return r;
    }

    private static DataFile writeRecords(Table table, List<Record> records) throws Exception {
        GenericAppenderFactory af = new GenericAppenderFactory(table.schema(), table.spec());
        String filename = "data-" + UUID.randomUUID() + ".parquet";
        OutputFile out = table.io().newOutputFile(table.location() + "/data/" + filename);

        DataWriter<Record> writer = af.newDataWriter(
                EncryptionUtil.plainAsEncryptedOutput(out),
                FileFormat.PARQUET,
                null);
        try (writer) {
            for (Record r : records) writer.write(r);
        }
        return writer.toDataFile();
    }

    /**
     * Write a v3 deletion-vector file targeting the given (data-file, position) pairs.
     * Uses Iceberg's BaseDVFileWriter which produces a Puffin file with a roaring bitmap blob.
     */
    private static DeleteFile writeDvFor(Table table, String dataFilePath, long[] positions) throws Exception {
        OutputFileFactory fileFactory = OutputFileFactory.builderFor(table, 0, 0)
                .format(FileFormat.PUFFIN)
                .build();
        org.apache.iceberg.deletes.BaseDVFileWriter dvWriter =
                new org.apache.iceberg.deletes.BaseDVFileWriter(fileFactory, p -> null);
        try (dvWriter) {
            for (long pos : positions) {
                dvWriter.delete(dataFilePath, pos, table.spec(), null);
            }
        }
        org.apache.iceberg.io.DeleteWriteResult result = dvWriter.result();
        return result.deleteFiles().get(0);
    }

    /** Write an equality-delete file targeting rows by id-value set. */
    private static DeleteFile writeEqualityDelete(Table table, List<Long> ids) throws Exception {
        org.apache.iceberg.types.Types.NestedField idField = table.schema().findField("id");
        Schema deleteSchema = new Schema(idField);
        int[] equalityFieldIds = new int[] { idField.fieldId() };

        org.apache.iceberg.data.GenericAppenderFactory af =
                new org.apache.iceberg.data.GenericAppenderFactory(
                        table.schema(), table.spec(), equalityFieldIds, deleteSchema, null);
        String filename = "eq-delete-" + UUID.randomUUID() + ".parquet";
        OutputFile out = table.io().newOutputFile(table.location() + "/data/" + filename);
        org.apache.iceberg.deletes.EqualityDeleteWriter<Record> writer =
                af.newEqDeleteWriter(EncryptionUtil.plainAsEncryptedOutput(out),
                        FileFormat.PARQUET, null);
        try (writer) {
            for (Long id : ids) {
                org.apache.iceberg.data.GenericRecord r =
                        org.apache.iceberg.data.GenericRecord.create(deleteSchema);
                r.setField("id", id);
                writer.write(r);
            }
        }
        return writer.toDeleteFile();
    }

    private static DeleteFile writeGlobalPositionDelete(
            Table table, Map<String, long[]> deletesByPath) throws Exception {
        GenericAppenderFactory af = new GenericAppenderFactory(table.schema(), table.spec());
        String filename = "pos-delete-" + UUID.randomUUID() + ".parquet";
        OutputFile out = table.io().newOutputFile(table.location() + "/data/" + filename);
        org.apache.iceberg.deletes.PositionDeleteWriter<Record> writer =
                af.newPosDeleteWriter(EncryptionUtil.plainAsEncryptedOutput(out),
                        FileFormat.PARQUET, null);
        try (writer) {
            for (Map.Entry<String, long[]> e : deletesByPath.entrySet()) {
                for (long pos : e.getValue()) {
                    writer.write(PositionDelete.<Record>create().set(e.getKey(), pos));
                }
            }
        }
        return writer.toDeleteFile();
    }

    private record EmittedRow(RowKind kind, long id, String value) {}

    /** Serializable table supplier — captures only strings. */
    private static final class SerializableTableSupplier
            implements io.mat.source.IcebergCdcEnumerator.TableSupplier {
        private static final long serialVersionUID = 1L;
        private final String warehouse;
        private final String namespace;
        private final String tableName;
        SerializableTableSupplier(String warehouse, String namespace, String tableName) {
            this.warehouse = warehouse;
            this.namespace = namespace;
            this.tableName = tableName;
        }
        @Override public Table get() {
            IcebergTestHarness h = new IcebergTestHarness(warehouse);
            return h.catalog().loadTable(TableIdentifier.of(namespace, tableName));
        }
    }
}
