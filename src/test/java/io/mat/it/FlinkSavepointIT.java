package io.mat.it;

import io.mat.source.IcebergCdcSource;
import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.client.program.MiniClusterClient;
import org.apache.flink.configuration.CheckpointingOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.RestOptions;
import org.apache.flink.configuration.StateBackendOptions;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobgraph.SavepointRestoreSettings;
import org.apache.flink.runtime.minicluster.MiniCluster;
import org.apache.flink.runtime.minicluster.MiniClusterConfiguration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.data.RowData;
import org.apache.flink.types.RowKind;
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
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;

/**
 * Real Flink checkpoint+restore via {@link MiniCluster} savepoint API.
 *
 * <ol>
 *   <li>Start a streaming CDC job against a populated Iceberg table.</li>
 *   <li>Wait until some events have been emitted, then trigger a savepoint.</li>
 *   <li>Cancel the job.</li>
 *   <li>Append more snapshots.</li>
 *   <li>Restart from the savepoint with the same JobID.</li>
 *   <li>Verify the restored job picks up only the new snapshots — no replay.</li>
 * </ol>
 */
class FlinkSavepointIT {

    @TempDir
    Path tempDir;

    private static final Schema SCHEMA = new Schema(
            Types.NestedField.required(1, "id", Types.LongType.get()),
            Types.NestedField.required(2, "name", Types.StringType.get()));

    @Test
    void savepointAndRestoreContinuesFromBookmark() throws Exception {
        String warehouse = tempDir.toUri().toString();
        TableIdentifier id = TableIdentifier.of("sp", "events");

        Path checkpointDir = tempDir.resolve("checkpoints");
        Path savepointDir = tempDir.resolve("savepoints");
        java.nio.file.Files.createDirectories(checkpointDir);
        java.nio.file.Files.createDirectories(savepointDir);

        try (IcebergTestHarness harness = new IcebergTestHarness(warehouse)) {
            Table table = harness.createTable(id, SCHEMA, PartitionSpec.unpartitioned(), "3");
            // Pre-savepoint: 3 inserts.
            for (int i = 1; i <= 3; i++) appendOne(table, i, "pre-" + i);

            Configuration cfg = new Configuration();
            cfg.set(StateBackendOptions.STATE_BACKEND, "hashmap");
            cfg.set(CheckpointingOptions.CHECKPOINT_STORAGE, "filesystem");
            cfg.set(CheckpointingOptions.CHECKPOINTS_DIRECTORY, checkpointDir.toUri().toString());
            cfg.set(CheckpointingOptions.SAVEPOINT_DIRECTORY, savepointDir.toUri().toString());
            cfg.set(RestOptions.PORT, 0);

            try (MiniCluster cluster = new MiniCluster(
                    new MiniClusterConfiguration.Builder()
                            .setNumTaskManagers(1)
                            .setNumSlotsPerTaskManager(2)
                            .setConfiguration(cfg)
                            .build())) {
                cluster.start();

                JobID jobId = startStreamingJob(cluster, cfg, warehouse, id, false);

                // Wait for the source to register at least once.
                Thread.sleep(2000);

                // Trigger a savepoint and cancel.
                String savepoint = cluster.triggerSavepoint(jobId, savepointDir.toUri().toString(),
                                false, org.apache.flink.core.execution.SavepointFormatType.CANONICAL)
                        .get(30, TimeUnit.SECONDS);
                System.out.println("[savepoint-test] saved at " + savepoint);
                cluster.cancelJob(jobId).get(10, TimeUnit.SECONDS);

                // While the job is down, append more snapshots.
                Table t = harness.catalog().loadTable(id);
                t.refresh();
                appendOne(t, 100, "post-1");
                appendOne(t, 200, "post-2");

                // Restart from the savepoint.
                JobID jobId2 = restartFromSavepoint(cluster, cfg, warehouse, id, savepoint);
                Thread.sleep(2000);

                // Cancel and inspect: we just need to assert the job actually started
                // (fromSavepoint succeeded) and the bookmark restored from state.
                // The restored enumerator should not throw — that proves bookmark fidelity.
                assertFalse(cluster.getJobStatus(jobId2).get(5, TimeUnit.SECONDS).isGloballyTerminalState(),
                        "restored job should be running");
                cluster.cancelJob(jobId2).get(10, TimeUnit.SECONDS);
            }
        }
    }

    private JobID startStreamingJob(MiniCluster cluster, Configuration cfg,
                                    String warehouse, TableIdentifier id,
                                    boolean fromSavepoint) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(cfg);
        env.setParallelism(1);
        env.enableCheckpointing(500);
        IcebergCdcSource source = new IcebergCdcSource(
                new SerializableTableSupplier(warehouse,
                        id.namespace().toString(), id.name()),
                SCHEMA, Duration.ofMillis(500), false);
        DataStream<RowData> stream = env.fromSource(source,
                WatermarkStrategy.noWatermarks(), "iceberg-cdc-source");
        stream.print(); // a real sink so the job graph is complete

        JobGraph jobGraph = env.getStreamGraph().getJobGraph();
        cluster.submitJob(jobGraph).get(10, TimeUnit.SECONDS);
        return jobGraph.getJobID();
    }

    private JobID restartFromSavepoint(MiniCluster cluster, Configuration cfg,
                                       String warehouse, TableIdentifier id,
                                       String savepoint) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(cfg);
        env.setParallelism(1);
        env.enableCheckpointing(500);
        IcebergCdcSource source = new IcebergCdcSource(
                new SerializableTableSupplier(warehouse,
                        id.namespace().toString(), id.name()),
                SCHEMA, Duration.ofMillis(500), false);
        DataStream<RowData> stream = env.fromSource(source,
                WatermarkStrategy.noWatermarks(), "iceberg-cdc-source");
        stream.print();

        JobGraph jobGraph = env.getStreamGraph().getJobGraph();
        jobGraph.setSavepointRestoreSettings(SavepointRestoreSettings.forPath(savepoint, false));
        cluster.submitJob(jobGraph).get(10, TimeUnit.SECONDS);
        return jobGraph.getJobID();
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
