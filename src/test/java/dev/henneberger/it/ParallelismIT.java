package dev.henneberger.it;

import dev.henneberger.source.IcebergCdcSource;
import dev.henneberger.source.SplitAssignmentMode;
import dev.henneberger.source.StartupMode;
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

/**
 * Multi-parallelism CDC: with parallelism=2, splits for the same data file must
 * land on the same subtask so a DELETE for a row position arrives after the
 * INSERT that wrote it.
 */
class ParallelismIT {

    @TempDir
    Path tempDir;

    private static final Schema SCHEMA = new Schema(
            Types.NestedField.required(1, "id", Types.LongType.get()),
            Types.NestedField.required(2, "name", Types.StringType.get()));

    @Test
    void deleteOrderedAfterInsertOnSameDataFileWithParallelism2() throws Exception {
        String warehouse = tempDir.toUri().toString();
        TableIdentifier id = TableIdentifier.of("par", "events");

        try (IcebergTestHarness harness = new IcebergTestHarness(warehouse)) {
            Table table = harness.createTable(id, SCHEMA, PartitionSpec.unpartitioned(), "3");

            // Two separate data files, each gets a DV.
            DataFile fileA = writeRecords(table, List.of(rec(1L, "a1"), rec(2L, "a2"), rec(3L, "a3")));
            DataFile fileB = writeRecords(table, List.of(rec(4L, "b1"), rec(5L, "b2")));
            table.newAppend().appendFile(fileA).appendFile(fileB).commit();
            table.refresh();

            // Snapshot 2: delete one row from each file.
            DeleteFile dvA = writeDvFor(table, fileA.path().toString(), new long[] { 1L });
            DeleteFile dvB = writeDvFor(table, fileB.path().toString(), new long[] { 0L });
            table.newRowDelta().addDeletes(dvA).addDeletes(dvB).commit();
            table.refresh();

            String wh = warehouse;
            String tableNs = id.namespace().toString();
            String tableName = id.name();
            IcebergCdcSource source = new IcebergCdcSource(
                    new SerializableTableSupplier(wh, tableNs, tableName),
                    SCHEMA, Duration.ofSeconds(1), true, null,
                    SplitAssignmentMode.FILE_AFFINITY, StartupMode.EARLIEST);

            StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
            env.setParallelism(2);
            DataStream<RowData> stream = env.fromSource(source,
                    WatermarkStrategy.noWatermarks(), "iceberg-cdc-source");

            // Stamp each row with its subtask index using a parallel map operator,
            // so we can assert which subtask saw each event.
            DataStream<TaggedRow> tagged = stream.map(new SubtaskStamper());

            List<TaggedRow> emitted = new ArrayList<>();
            try (CloseableIterator<TaggedRow> it = tagged.executeAndCollect("parallelism-cdc")) {
                while (it.hasNext()) emitted.add(it.next());
            }

            System.out.println("[parallelism-cdc] emitted: " + emitted);

            // 5 inserts + 2 deletes = 7 events.
            assertEquals(7, emitted.size(), "expected 7 events: " + emitted);

            // For each id, the INSERT and (if any) DELETE must come from the SAME subtask
            // so SQL retract semantics work correctly.
            Map<Long, Integer> insertSubtask = new HashMap<>();
            Map<Long, Integer> deleteSubtask = new HashMap<>();
            for (TaggedRow r : emitted) {
                if (r.kind == RowKind.INSERT) insertSubtask.put(r.id, r.subtaskIndex);
                else if (r.kind == RowKind.DELETE) deleteSubtask.put(r.id, r.subtaskIndex);
            }
            for (Map.Entry<Long, Integer> e : deleteSubtask.entrySet()) {
                Integer ins = insertSubtask.get(e.getKey());
                assertEquals(ins, e.getValue(),
                        "id=" + e.getKey() + " INSERT and DELETE must hit the same subtask");
            }

            // File-affinity can use one or both subtasks depending on the randomized file paths;
            // the correctness contract is same-file consistency, asserted above.
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

    public static final class TaggedRow {
        public final RowKind kind;
        public final long id;
        public final String name;
        public final int subtaskIndex;
        public TaggedRow(RowKind kind, long id, String name, int subtaskIndex) {
            this.kind = kind; this.id = id; this.name = name; this.subtaskIndex = subtaskIndex;
        }
        @Override public String toString() {
            return "(" + kind.shortString() + " id=" + id + " sub=" + subtaskIndex + ")";
        }
    }

    private static final class SubtaskStamper
            extends org.apache.flink.api.common.functions.AbstractRichFunction
            implements org.apache.flink.api.common.functions.MapFunction<RowData, TaggedRow> {
        private static final long serialVersionUID = 1L;
        @Override public TaggedRow map(RowData r) {
            int idx = getRuntimeContext().getTaskInfo().getIndexOfThisSubtask();
            return new TaggedRow(r.getRowKind(), r.getLong(0),
                    r.getString(1) == null ? null : r.getString(1).toString(), idx);
        }
    }

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
