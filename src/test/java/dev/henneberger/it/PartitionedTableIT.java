package dev.henneberger.it;

import dev.henneberger.source.IcebergCdcSource;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.data.RowData;
import org.apache.flink.types.RowKind;
import org.apache.flink.util.CloseableIterator;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.PartitionData;
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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Partitioned table CDC: data files have a real PartitionSpec; positional/DV deletes
 * scope to that partition. Reader must reconstruct delete files with the right spec
 * (not unpartitioned) so they apply correctly.
 */
class PartitionedTableIT {

    @TempDir
    Path tempDir;

    private static final Schema SCHEMA = new Schema(
            Types.NestedField.required(1, "id", Types.LongType.get()),
            Types.NestedField.required(2, "region", Types.StringType.get()),
            Types.NestedField.required(3, "name", Types.StringType.get()));

    @Test
    void partitionedEqualityDeleteScopesToOnePartition() throws Exception {
        // Equality delete on `id` written into partition `region=us`. It must NOT match
        // identical id values in the `region=eu` partition. This exercises
        // findDataFilesAffectedByEqualityDelete's partition filtering.
        String warehouse = tempDir.toUri().toString();
        TableIdentifier id = TableIdentifier.of("part", "eqdel");

        try (IcebergTestHarness harness = new IcebergTestHarness(warehouse)) {
            PartitionSpec spec = PartitionSpec.builderFor(SCHEMA).identity("region").build();
            Table table = harness.createTable(id, SCHEMA, spec, "2"); // v2 for equality deletes

            // Snapshot 1: insert id=1 in BOTH partitions.
            DataFile us = writePartitionedRecords(table, "us", List.of(rec(1L, "us", "alice")));
            DataFile eu = writePartitionedRecords(table, "eu", List.of(rec(1L, "eu", "alex")));
            table.newAppend().appendFile(us).appendFile(eu).commit();
            table.refresh();

            // Snapshot 2: equality-delete id=1, scoped to partition us only.
            DeleteFile eqDel = writeEqualityDeleteForPartition(table, "us", List.of(1L));
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
            try (CloseableIterator<RowData> it = stream.executeAndCollect("partitioned-eq")) {
                while (it.hasNext()) {
                    RowData row = it.next();
                    emitted.add(new EmittedRow(row.getRowKind(), row.getLong(0),
                            row.getString(1).toString(), row.getString(2).toString()));
                }
            }
            System.out.println("[partitioned-eq] emitted: " + emitted);

            // 2 INSERTs (one per partition), 1 DELETE (only us-partition matches).
            assertEquals(3, emitted.size());
            long deletes = emitted.stream().filter(r -> r.kind == RowKind.DELETE).count();
            assertEquals(1, deletes, "only the us-partition row gets deleted: " + emitted);
            EmittedRow del = emitted.stream().filter(r -> r.kind == RowKind.DELETE)
                    .findFirst().orElseThrow();
            assertEquals("us", del.region, "delete must scope to us");
            assertEquals("alice", del.name);
            // The eu partition's id=1 row must remain alive in the changelog (no DELETE for it).
            long euDeletes = emitted.stream()
                    .filter(r -> r.region.equals("eu") && r.kind == RowKind.DELETE).count();
            assertEquals(0, euDeletes, "eu partition must NOT receive the delete");
        }
    }

    @Test
    void partitionedTableInsertsAndDvDeletes() throws Exception {
        String warehouse = tempDir.toUri().toString();
        TableIdentifier id = TableIdentifier.of("part", "events");

        try (IcebergTestHarness harness = new IcebergTestHarness(warehouse)) {
            PartitionSpec spec = PartitionSpec.builderFor(SCHEMA).identity("region").build();
            Table table = harness.createTable(id, SCHEMA, spec, "3");

            // Two partitions, each its own data file.
            DataFile us = writePartitionedRecords(table, "us", List.of(
                    rec(1L, "us", "alice"), rec(2L, "us", "bob")));
            DataFile eu = writePartitionedRecords(table, "eu", List.of(
                    rec(3L, "eu", "carol"), rec(4L, "eu", "dave")));
            table.newAppend().appendFile(us).appendFile(eu).commit();
            table.refresh();

            // DV delete on us partition (id=1, position 0).
            DeleteFile dv = writeDvFor(table, us.path().toString(), new long[] { 0L });
            table.newRowDelta().addDeletes(dv).commit();
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
            try (CloseableIterator<RowData> it = stream.executeAndCollect("partitioned")) {
                while (it.hasNext()) {
                    RowData row = it.next();
                    emitted.add(new EmittedRow(row.getRowKind(), row.getLong(0),
                            row.getString(1).toString(), row.getString(2).toString()));
                }
            }

            assertEquals(5, emitted.size(), "4 inserts + 1 delete: " + emitted);
            long inserts = emitted.stream().filter(r -> r.kind == RowKind.INSERT).count();
            long deletes = emitted.stream().filter(r -> r.kind == RowKind.DELETE).count();
            assertEquals(4, inserts);
            assertEquals(1, deletes);

            EmittedRow del = emitted.stream().filter(r -> r.kind == RowKind.DELETE)
                    .findFirst().orElseThrow();
            assertEquals(1L, del.id, "DV at us partition deletes id=1");
            assertEquals("us", del.region);

            // EU partition rows must all still be present (DV scoped to us only).
            long euAlive = emitted.stream()
                    .filter(r -> r.region.equals("eu") && r.kind == RowKind.INSERT)
                    .count();
            assertEquals(2, euAlive, "eu partition unaffected");
        }
    }

    private static Record rec(long id, String region, String name) {
        GenericRecord r = GenericRecord.create(SCHEMA);
        r.setField("id", id);
        r.setField("region", region);
        r.setField("name", name);
        return r;
    }

    private static DeleteFile writeEqualityDeleteForPartition(Table table, String regionVal,
                                                             List<Long> ids) throws Exception {
        org.apache.iceberg.types.Types.NestedField idField = table.schema().findField("id");
        Schema deleteSchema = new Schema(idField);
        int[] equalityFieldIds = new int[] { idField.fieldId() };

        PartitionData partition = new PartitionData(table.spec().partitionType());
        partition.set(0, regionVal);

        GenericAppenderFactory af = new GenericAppenderFactory(
                table.schema(), table.spec(), equalityFieldIds, deleteSchema, null);
        String filename = "eq-" + regionVal + "-" + UUID.randomUUID() + ".parquet";
        OutputFile out = table.io().newOutputFile(table.location() + "/data/" + filename);
        org.apache.iceberg.deletes.EqualityDeleteWriter<Record> writer =
                af.newEqDeleteWriter(EncryptionUtil.plainAsEncryptedOutput(out),
                        FileFormat.PARQUET, partition);
        try (writer) {
            for (Long idVal : ids) {
                GenericRecord r = GenericRecord.create(deleteSchema);
                r.setField("id", idVal);
                writer.write(r);
            }
        }
        return writer.toDeleteFile();
    }

    private static DataFile writePartitionedRecords(Table table, String regionVal,
                                                    List<Record> records) throws Exception {
        PartitionData partition = new PartitionData(table.spec().partitionType());
        partition.set(0, regionVal);
        GenericAppenderFactory af = new GenericAppenderFactory(table.schema(), table.spec());
        String filename = "data-" + regionVal + "-" + UUID.randomUUID() + ".parquet";
        OutputFile out = table.io().newOutputFile(table.location() + "/data/" + filename);
        DataWriter<Record> writer = af.newDataWriter(
                EncryptionUtil.plainAsEncryptedOutput(out),
                FileFormat.PARQUET, partition);
        try (writer) { for (Record r : records) writer.write(r); }
        return writer.toDataFile();
    }

    private static DeleteFile writeDvFor(Table table, String dataFilePath, long[] positions) throws Exception {
        OutputFileFactory fileFactory = OutputFileFactory.builderFor(table, 0, 0)
                .format(FileFormat.PUFFIN).build();
        BaseDVFileWriter dvWriter = new BaseDVFileWriter(fileFactory, p -> null);
        // Use the spec for the data file's partition; partition value matters for DV scoping.
        PartitionData partition = null;
        for (DataFile df : table.currentSnapshot().addedDataFiles(table.io())) {
            if (df.path().toString().equals(dataFilePath)) {
                partition = (PartitionData) df.partition();
                break;
            }
        }
        try (dvWriter) {
            for (long pos : positions) dvWriter.delete(dataFilePath, pos, table.spec(), partition);
        }
        return dvWriter.result().deleteFiles().get(0);
    }

    private record EmittedRow(RowKind kind, long id, String region, String name) {}

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
