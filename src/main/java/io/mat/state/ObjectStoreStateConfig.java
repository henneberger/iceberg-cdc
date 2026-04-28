package io.mat.state;

import org.apache.flink.configuration.CheckpointingOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.StateBackendOptions;
import org.apache.flink.configuration.StateChangelogOptions;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.time.Duration;

/**
 * Wires Flink to put 100% of state on object storage.
 *
 * <ul>
 *   <li>RocksDB on-disk state is local (it has to be), but every byte ends up
 *       in incremental checkpoints uploaded to object storage.</li>
 *   <li>Changelog state backend continuously WALs state mutations to object
 *       storage between checkpoints — so TM crash recovery does not roll
 *       back any acked progress.</li>
 * </ul>
 *
 * <p>The {@code basePath} can be {@code s3://bucket/...}, {@code gs://...},
 * {@code abfs://...}, or {@code file://...} for tests. The choice is purely
 * Flink {@code FileSystem} URI scheme — same wiring works everywhere.
 *
 * <p>Use this from a streaming entrypoint that creates a {@link StreamExecutionEnvironment}
 * directly, OR pass the underlying {@link Configuration} to {@code StreamExecutionEnvironment.getExecutionEnvironment(conf)}
 * so {@code StreamTableEnvironment.create(env)} inherits it.
 */
public final class ObjectStoreStateConfig {

    private ObjectStoreStateConfig() {}

    public static Configuration buildConfig(String basePath) {
        Configuration conf = new Configuration();
        // State backend — RocksDB so working state is on disk (production); for unit
        // tests where JNI may not be available, switch via {@code -Dmat.state.backend=hashmap}.
        String backend = System.getProperty("mat.state.backend", "rocksdb");
        conf.set(StateBackendOptions.STATE_BACKEND, backend);
        if ("rocksdb".equals(backend)) {
            conf.set(CheckpointingOptions.INCREMENTAL_CHECKPOINTS, true);
        }

        // Checkpoint storage explicitly = filesystem (this is the magic key — without
        // it, MiniCluster defaults to "jobmanager" in-memory checkpoint storage and
        // ignores the dir).
        conf.setString("state.checkpoint-storage", "filesystem");
        conf.set(CheckpointingOptions.CHECKPOINTS_DIRECTORY, basePath + "/checkpoints");
        conf.set(CheckpointingOptions.SAVEPOINT_DIRECTORY, basePath + "/savepoints");

        // Continuous-WAL state changelog → object storage. This is what actually makes
        // mid-checkpoint state durable.
        conf.set(StateChangelogOptions.ENABLE_STATE_CHANGE_LOG, true);
        conf.setString("state.backend.changelog.storage", "filesystem");
        conf.setString("dstl.dfs.base-path", basePath + "/changelog");
        conf.setString("dstl.dfs.batch.persist-delay", "10ms");

        // For S3 specifically; harmless for file://.
        conf.setString("fs.s3.path.style.access", "true");
        return conf;
    }

    public static void apply(StreamExecutionEnvironment env, String basePath, Duration checkpointInterval) {
        env.configure(buildConfig(basePath));

        env.enableCheckpointing(checkpointInterval.toMillis());
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(checkpointInterval.toMillis() / 2);
        env.getCheckpointConfig().setCheckpointTimeout(Duration.ofMinutes(10).toMillis());
        env.getCheckpointConfig().enableUnalignedCheckpoints(false); // event-time correctness > speed
        env.getCheckpointConfig().setExternalizedCheckpointRetention(
                org.apache.flink.configuration.ExternalizedCheckpointRetention.RETAIN_ON_CANCELLATION);
    }
}
