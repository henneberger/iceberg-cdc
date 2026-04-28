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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Schema evolution: data files written before a column was added should still
 * be readable. Iceberg projects by field id and fills missing columns with null.
 */
class SchemaEvolutionIT {

    @TempDir
    Path tempDir;

    @Test
    void readerHandlesAddColumnAcrossSnapshots() throws Exception {
        String warehouse = tempDir.toUri().toString();
        TableIdentifier id = TableIdentifier.of("schema", "evolved");

        Schema initial = new Schema(
                Types.NestedField.required(1, "id", Types.LongType.get()),
                Types.NestedField.required(2, "name", Types.StringType.get()));

        try (IcebergTestHarness harness = new IcebergTestHarness(warehouse)) {
            Table table = harness.createTable(id, initial, PartitionSpec.unpartitioned(), "3");

            // Snapshot 1: write 2 rows with 2 columns.
            DataFile file1 = writeRecords(table, initial, List.of(
                    rec(initial, 1L, "a", null), rec(initial, 2L, "b", null)));
            table.newAppend().appendFile(file1).commit();
            table.refresh();

            // Evolve schema: add nullable column 'amount'.
            table.updateSchema().addColumn("amount", Types.DoubleType.get()).commit();
            table.refresh();
            Schema evolved = table.schema();

            // Snapshot 2: write a row with all 3 columns.
            DataFile file2 = writeRecords(table, evolved, List.of(
                    rec(evolved, 3L, "c", 9.5)));
            table.newAppend().appendFile(file2).commit();
            table.refresh();

            // Read the CDC stream with the evolved schema (3 columns).
            String wh = warehouse;
            String tableNs = id.namespace().toString();
            String tableName = id.name();
            IcebergCdcSource source = new IcebergCdcSource(
                    new SerializableTableSupplier(wh, tableNs, tableName),
                    evolved, Duration.ofSeconds(1), true);

            StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
            env.setParallelism(1);
            DataStream<RowData> stream = env.fromSource(source,
                    WatermarkStrategy.noWatermarks(), "iceberg-cdc-source");

            List<Row> emitted = new ArrayList<>();
            try (CloseableIterator<RowData> it = stream.executeAndCollect("schema-evolution")) {
                while (it.hasNext()) {
                    RowData r = it.next();
                    emitted.add(new Row(
                            r.getRowKind(),
                            r.getLong(0),
                            r.getString(1) == null ? null : r.getString(1).toString(),
                            r.isNullAt(2) ? null : r.getDouble(2)));
                }
            }
            System.out.println("[schema-evolution] emitted: " + emitted);

            // Expected: 3 inserts. Row 1 and 2 from old schema have amount=null;
            // row 3 from new schema has amount=9.5.
            assertEquals(3, emitted.size());
            assertEquals(3, emitted.stream().filter(r -> r.kind == RowKind.INSERT).count());
            Row r1 = emitted.stream().filter(r -> r.id == 1L).findFirst().orElseThrow();
            assertEquals(null, r1.amount, "row written before column added must read as null");
            Row r3 = emitted.stream().filter(r -> r.id == 3L).findFirst().orElseThrow();
            assertEquals(9.5, r3.amount);
        }
    }

    private static Record rec(Schema schema, long id, String name, Double amount) {
        GenericRecord r = GenericRecord.create(schema);
        r.setField("id", id);
        r.setField("name", name);
        if (schema.findField("amount") != null) r.setField("amount", amount);
        return r;
    }

    private static DataFile writeRecords(Table table, Schema schema, List<Record> records) throws Exception {
        GenericAppenderFactory af = new GenericAppenderFactory(schema, table.spec());
        String filename = "data-" + UUID.randomUUID() + ".parquet";
        OutputFile out = table.io().newOutputFile(table.location() + "/data/" + filename);
        DataWriter<Record> writer = af.newDataWriter(
                EncryptionUtil.plainAsEncryptedOutput(out),
                FileFormat.PARQUET, null);
        try (writer) { for (Record r : records) writer.write(r); }
        return writer.toDataFile();
    }

    private record Row(RowKind kind, long id, String name, Double amount) {}

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
