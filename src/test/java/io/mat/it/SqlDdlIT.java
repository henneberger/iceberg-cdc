package io.mat.it;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
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
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Real Flink SQL → connector SPI → DynamicTableSource → CDC source → emitted rows.
 * Verifies the connector is registered and usable as a SQL DDL.
 */
class SqlDdlIT {

    @TempDir
    Path tempDir;

    private static final Schema SCHEMA = new Schema(
            Types.NestedField.required(1, "id", Types.LongType.get()),
            Types.NestedField.required(2, "name", Types.StringType.get()));

    @Test
    void sqlEqualityDeleteWithProjectionDropsKey() throws Exception {
        // Regression: equality deletes are keyed on `id`. SELECT name FROM t — `id` is not
        // in the projection. The reader must still read `id` for the equality match and emit
        // only `name`. (Previously threw because it looked up equality field id in projected schema.)
        String warehouse = tempDir.toUri().toString();
        TableIdentifier id = TableIdentifier.of("sql", "eqproj");

        try (IcebergTestHarness harness = new IcebergTestHarness(warehouse)) {
            Table table = harness.createTable(id, SCHEMA, PartitionSpec.unpartitioned(), "2");
            DataFile file1 = writeRecords(table, List.of(rec(1L, "a"), rec(2L, "b"), rec(3L, "c")));
            table.newAppend().appendFile(file1).commit();
            table.refresh();

            DeleteFile eqDel = writeEqualityDeleteOnId(table, List.of(2L));
            table.newRowDelta().addDeletes(eqDel).commit();
            table.refresh();

            StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
            env.setParallelism(1);
            EnvironmentSettings settings = EnvironmentSettings.newInstance().inStreamingMode().build();
            StreamTableEnvironment tEnv = StreamTableEnvironment.create(env, settings);

            tEnv.executeSql(""
                    + "CREATE TABLE eqproj ("
                    + "  id BIGINT, name STRING"
                    + ") WITH ("
                    + "  'connector' = 'iceberg-cdc',"
                    + "  'catalog-impl' = 'org.apache.iceberg.hadoop.HadoopCatalog',"
                    + "  'warehouse' = '" + warehouse + "',"
                    + "  'table' = 'sql.eqproj',"
                    + "  'discovery-interval' = '1 s',"
                    + "  'bounded' = 'true'"
                    + ")");

            // Project only `name` — equality key `id` is NOT selected.
            List<Row> rows = new ArrayList<>();
            try (CloseableIterator<Row> it = tEnv.executeSql("SELECT name FROM eqproj").collect()) {
                while (it.hasNext()) rows.add(it.next());
            }
            // 3 inserts + 1 delete = 4 rows. The DELETE must be `name=b` (the row where id=2).
            assertEquals(4, rows.size(), "4 changelog events: " + rows);
            long deletes = rows.stream().filter(r -> r.getKind().shortString().equals("-D")).count();
            assertEquals(1, deletes);
            Row del = rows.stream().filter(r -> r.getKind().shortString().equals("-D"))
                    .findFirst().orElseThrow();
            assertEquals("b", del.getField(0));
        }
    }

    @Test
    void sqlSelectsLineageColumnsWhenDeclared() throws Exception {
        // Regression: user declares _row_id / _last_updated_sequence_number in DDL.
        // Reader must use schemaWithRowLineage so Iceberg populates them.
        String warehouse = tempDir.toUri().toString();
        TableIdentifier id = TableIdentifier.of("sql", "lineage");

        try (IcebergTestHarness harness = new IcebergTestHarness(warehouse)) {
            Table table = harness.createTable(id, SCHEMA, PartitionSpec.unpartitioned(), "3");
            // Enable row-lineage on the table so Iceberg actually maintains the columns.
            table.updateProperties().set("write.row-lineage.enabled", "true").commit();

            DataFile file1 = writeRecords(table, List.of(rec(1L, "a"), rec(2L, "b")));
            table.newAppend().appendFile(file1).commit();
            table.refresh();

            StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
            env.setParallelism(1);
            EnvironmentSettings settings = EnvironmentSettings.newInstance().inStreamingMode().build();
            StreamTableEnvironment tEnv = StreamTableEnvironment.create(env, settings);

            tEnv.executeSql(""
                    + "CREATE TABLE lineage ("
                    + "  id BIGINT, name STRING,"
                    + "  _last_updated_sequence_number BIGINT"
                    + ") WITH ("
                    + "  'connector' = 'iceberg-cdc',"
                    + "  'catalog-impl' = 'org.apache.iceberg.hadoop.HadoopCatalog',"
                    + "  'warehouse' = '" + warehouse + "',"
                    + "  'table' = 'sql.lineage',"
                    + "  'discovery-interval' = '1 s',"
                    + "  'bounded' = 'true'"
                    + ")");

            // The point of this regression test is that the DDL with lineage cols is
            // actually pluggable end-to-end without throwing.
            List<Row> rows = new ArrayList<>();
            try (CloseableIterator<Row> it = tEnv.executeSql(
                    "SELECT id, _last_updated_sequence_number FROM lineage").collect()) {
                while (it.hasNext()) rows.add(it.next());
            }
            assertEquals(2, rows.size());
            // The CDC reader synthesizes this metadata from the split's data sequence.
            for (Row r : rows) {
                Object seq = r.getField(1);
                assertTrue(seq != null && ((Number) seq).longValue() >= 0,
                        "_last_updated_sequence_number must be non-null and >= 0, got: " + seq);
            }
        }
    }

    @Test
    void sqlReadsIcebergMetadataColumnsWithMetadataSyntax() throws Exception {
        String warehouse = tempDir.toUri().toString();
        TableIdentifier id = TableIdentifier.of("sql", "metadata_lineage");

        try (IcebergTestHarness harness = new IcebergTestHarness(warehouse)) {
            Table table = harness.createTable(id, SCHEMA, PartitionSpec.unpartitioned(), "3");
            DataFile file1 = writeRecords(table, List.of(rec(1L, "a"), rec(2L, "b")));
            table.newAppend().appendFile(file1).commit();
            table.refresh();

            StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
            env.setParallelism(1);
            EnvironmentSettings settings = EnvironmentSettings.newInstance().inStreamingMode().build();
            StreamTableEnvironment tEnv = StreamTableEnvironment.create(env, settings);

            tEnv.executeSql(""
                    + "CREATE TABLE metadata_lineage ("
                    + "  id BIGINT, name STRING,"
                    + "  seq BIGINT METADATA FROM '_last_updated_sequence_number',"
                    + "  snap BIGINT METADATA FROM '_commit_snapshot_id',"
                    + "  committed_at TIMESTAMP_LTZ(3) METADATA FROM '_commit_timestamp',"
                    + "  kind STRING METADATA FROM '_change_type',"
                    + "  file_path STRING METADATA FROM '_file'"
                    + ") WITH ("
                    + "  'connector' = 'iceberg-cdc',"
                    + "  'catalog-impl' = 'org.apache.iceberg.hadoop.HadoopCatalog',"
                    + "  'warehouse' = '" + warehouse + "',"
                    + "  'table' = 'sql.metadata_lineage',"
                    + "  'discovery-interval' = '1 s',"
                    + "  'bounded' = 'true'"
                    + ")");

            List<Row> rows = new ArrayList<>();
            try (CloseableIterator<Row> it = tEnv.executeSql(
                    "SELECT id, seq, snap, committed_at, kind, file_path FROM metadata_lineage").collect()) {
                while (it.hasNext()) rows.add(it.next());
            }

            assertEquals(2, rows.size(), "metadata rows: " + rows);
            for (Row r : rows) {
                assertTrue(((Number) r.getField(1)).longValue() >= 0,
                        "sequence number must be non-negative: " + r);
                assertTrue(((Number) r.getField(2)).longValue() > 0,
                        "snapshot id must be populated: " + r);
                assertTrue(r.getField(3) != null, "commit timestamp must be populated: " + r);
                assertEquals("INSERT", r.getField(4));
                assertTrue(r.getField(5) != null && r.getField(5).toString().contains("/data/"),
                        "file path metadata must be populated: " + r);
            }
        }
    }

    @Test
    void sqlStartupLatestSkipsExistingSnapshotsWhenBounded() throws Exception {
        String warehouse = tempDir.toUri().toString();
        TableIdentifier id = TableIdentifier.of("sql", "latest");

        try (IcebergTestHarness harness = new IcebergTestHarness(warehouse)) {
            Table table = harness.createTable(id, SCHEMA, PartitionSpec.unpartitioned(), "3");
            DataFile file1 = writeRecords(table, List.of(rec(1L, "a"), rec(2L, "b")));
            table.newAppend().appendFile(file1).commit();
            table.refresh();

            StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
            env.setParallelism(1);
            EnvironmentSettings settings = EnvironmentSettings.newInstance().inStreamingMode().build();
            StreamTableEnvironment tEnv = StreamTableEnvironment.create(env, settings);

            tEnv.executeSql(""
                    + "CREATE TABLE latest ("
                    + "  id BIGINT, name STRING"
                    + ") WITH ("
                    + "  'connector' = 'iceberg-cdc',"
                    + "  'catalog-impl' = 'org.apache.iceberg.hadoop.HadoopCatalog',"
                    + "  'warehouse' = '" + warehouse + "',"
                    + "  'table' = 'sql.latest',"
                    + "  'discovery-interval' = '1 s',"
                    + "  'scan.startup.mode' = 'latest',"
                    + "  'bounded' = 'true'"
                    + ")");

            List<Row> rows = new ArrayList<>();
            try (CloseableIterator<Row> it = tEnv.executeSql("SELECT id FROM latest").collect()) {
                while (it.hasNext()) rows.add(it.next());
            }
            assertEquals(List.of(), rows, "latest startup should bookmark existing snapshots");
        }
    }

    @Test
    void sqlRejectsIncompatibleDdlColumnType() throws Exception {
        String warehouse = tempDir.toUri().toString();
        TableIdentifier id = TableIdentifier.of("sql", "bad_type");

        try (IcebergTestHarness harness = new IcebergTestHarness(warehouse)) {
            Table table = harness.createTable(id, SCHEMA, PartitionSpec.unpartitioned(), "3");
            DataFile file1 = writeRecords(table, List.of(rec(1L, "a")));
            table.newAppend().appendFile(file1).commit();
            table.refresh();

            StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
            env.setParallelism(1);
            EnvironmentSettings settings = EnvironmentSettings.newInstance().inStreamingMode().build();
            StreamTableEnvironment tEnv = StreamTableEnvironment.create(env, settings);

            tEnv.executeSql(""
                    + "CREATE TABLE bad_type ("
                    + "  id STRING, name STRING"
                    + ") WITH ("
                    + "  'connector' = 'iceberg-cdc',"
                    + "  'catalog-impl' = 'org.apache.iceberg.hadoop.HadoopCatalog',"
                    + "  'warehouse' = '" + warehouse + "',"
                    + "  'table' = 'sql.bad_type',"
                    + "  'discovery-interval' = '1 s',"
                    + "  'bounded' = 'true'"
                    + ")");

            try (CloseableIterator<Row> it = tEnv.executeSql("SELECT id FROM bad_type").collect()) {
                while (it.hasNext()) it.next();
                throw new AssertionError("expected incompatible DDL type to fail");
            } catch (Exception e) {
                Throwable root = e;
                while (root.getCause() != null) root = root.getCause();
                assertTrue(String.valueOf(root.getMessage()).contains("has type"),
                        "expected type-validation error, got: " + root);
            }
        }
    }

    @Test
    void sqlSelectWithProjectionPushdown() throws Exception {
        String warehouse = tempDir.toUri().toString();
        TableIdentifier id = TableIdentifier.of("sql", "proj");

        try (IcebergTestHarness harness = new IcebergTestHarness(warehouse)) {
            Table table = harness.createTable(id, SCHEMA, PartitionSpec.unpartitioned(), "3");
            DataFile file1 = writeRecords(table, List.of(rec(1L, "a"), rec(2L, "b")));
            table.newAppend().appendFile(file1).commit();
            table.refresh();

            StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
            env.setParallelism(1);
            EnvironmentSettings settings = EnvironmentSettings.newInstance().inStreamingMode().build();
            StreamTableEnvironment tEnv = StreamTableEnvironment.create(env, settings);

            tEnv.executeSql(""
                    + "CREATE TABLE proj ("
                    + "  id BIGINT, name STRING"
                    + ") WITH ("
                    + "  'connector' = 'iceberg-cdc',"
                    + "  'catalog-impl' = 'org.apache.iceberg.hadoop.HadoopCatalog',"
                    + "  'warehouse' = '" + warehouse + "',"
                    + "  'table' = 'sql.proj',"
                    + "  'discovery-interval' = '1 s',"
                    + "  'bounded' = 'true'"
                    + ")");

            // Project only `name` — pushdown should narrow what the source reads.
            List<Row> rows = new ArrayList<>();
            try (CloseableIterator<Row> it = tEnv.executeSql("SELECT name FROM proj").collect()) {
                while (it.hasNext()) rows.add(it.next());
            }
            assertEquals(2, rows.size());
            assertEquals(1, rows.get(0).getArity(), "projected row must have arity 1");
            assertEquals("a", rows.get(0).getField(0));
            assertEquals("b", rows.get(1).getField(0));
        }
    }

    @Test
    void sqlDdlReadsCdcStreamFromV3Table() throws Exception {
        String warehouse = tempDir.toUri().toString();
        TableIdentifier id = TableIdentifier.of("sql", "events");

        try (IcebergTestHarness harness = new IcebergTestHarness(warehouse)) {
            Table table = harness.createTable(id, SCHEMA, PartitionSpec.unpartitioned(), "3");

            // Snapshot 1: insert id=1,2,3
            DataFile file1 = writeRecords(table, List.of(
                    rec(1L, "a"), rec(2L, "b"), rec(3L, "c")));
            table.newAppend().appendFile(file1).commit();
            table.refresh();

            // Snapshot 2: delete id=2 via DV.
            DeleteFile dv = writeDvFor(table, file1.path().toString(), new long[] { 1L });
            table.newRowDelta().addDeletes(dv).commit();
            table.refresh();

            // ---- Run via SQL DDL ----
            StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
            env.setParallelism(1);
            EnvironmentSettings settings = EnvironmentSettings.newInstance().inStreamingMode().build();
            StreamTableEnvironment tEnv = StreamTableEnvironment.create(env, settings);

            tEnv.executeSql(""
                    + "CREATE TABLE events ("
                    + "  id BIGINT, name STRING"
                    + ") WITH ("
                    + "  'connector' = 'iceberg-cdc',"
                    + "  'catalog-impl' = 'org.apache.iceberg.hadoop.HadoopCatalog',"
                    + "  'warehouse' = '" + warehouse + "',"
                    + "  'table' = 'sql.events',"
                    + "  'discovery-interval' = '1 s',"
                    + "  'allowed-lateness' = '1 s',"
                    + "  'bounded' = 'true'"
                    + ")");

            List<Row> rows = new ArrayList<>();
            try (CloseableIterator<Row> it = tEnv.executeSql("SELECT id, name FROM events").collect()) {
                while (it.hasNext()) rows.add(it.next());
            }

            System.out.println("[sql-ddl-test] rows: " + rows);

            // SQL planner with ChangelogMode.all() returns rows including their RowKind.
            // Without a sink that handles retract semantics, executeSql collect surfaces
            // them with row kinds: +I/+I/+I/-D = 4 rows.
            assertEquals(4, rows.size(), "expected 4 changelog rows: " + rows);

            long inserts = rows.stream().filter(r -> r.getKind().shortString().equals("+I")).count();
            long deletes = rows.stream().filter(r -> r.getKind().shortString().equals("-D")).count();
            assertEquals(3, inserts, "3 inserts");
            assertEquals(1, deletes, "1 delete");
        }
    }

    // --- helpers (same as CdcEndToEndIT but local to keep tests self-contained) ---

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

    private static DeleteFile writeEqualityDeleteOnId(Table table, List<Long> ids) throws Exception {
        org.apache.iceberg.types.Types.NestedField idField = table.schema().findField("id");
        Schema deleteSchema = new Schema(idField);
        int[] equalityFieldIds = new int[] { idField.fieldId() };

        GenericAppenderFactory af = new GenericAppenderFactory(
                table.schema(), table.spec(), equalityFieldIds, deleteSchema, null);
        String filename = "eq-" + UUID.randomUUID() + ".parquet";
        OutputFile out = table.io().newOutputFile(table.location() + "/data/" + filename);
        org.apache.iceberg.deletes.EqualityDeleteWriter<Record> writer =
                af.newEqDeleteWriter(EncryptionUtil.plainAsEncryptedOutput(out),
                        FileFormat.PARQUET, null);
        try (writer) {
            for (Long idVal : ids) {
                GenericRecord r = GenericRecord.create(deleteSchema);
                r.setField("id", idVal);
                writer.write(r);
            }
        }
        return writer.toDeleteFile();
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
}
