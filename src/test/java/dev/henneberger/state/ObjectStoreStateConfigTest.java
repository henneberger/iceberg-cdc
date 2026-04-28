package dev.henneberger.state;

import org.apache.flink.configuration.CheckpointingOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.StateBackendOptions;
import org.apache.flink.configuration.StateChangelogOptions;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Unit-tests that {@link ObjectStoreStateConfig#buildConfig} sets the keys
 * Flink 1.20 actually consults at job-manager startup. Without these the
 * MiniCluster silently defaults to in-memory state, which would break the
 * "all state on object storage" promise.
 */
class ObjectStoreStateConfigTest {

    @Test
    void setsStateBackendAndCheckpointStorage() {
        Configuration c = ObjectStoreStateConfig.buildConfig("s3://bucket/mat");
        // State backend (rocksdb in prod, hashmap when overridden by sysprop)
        String backend = c.get(StateBackendOptions.STATE_BACKEND);
        assertTrue(backend.equals("rocksdb") || backend.equals("hashmap"),
                "state backend must be rocksdb (default) or hashmap (override). got: " + backend);
        // Checkpoint storage MUST be 'filesystem' or MiniCluster falls back to jobmanager.
        assertEquals("filesystem", c.getString("state.checkpoint-storage", null));
        // Checkpoint and savepoint directories under the supplied base.
        assertEquals("s3://bucket/mat/checkpoints",
                c.get(CheckpointingOptions.CHECKPOINTS_DIRECTORY));
        assertEquals("s3://bucket/mat/savepoints",
                c.get(CheckpointingOptions.SAVEPOINT_DIRECTORY));
    }

    @Test
    void enablesChangelogStateBackendAgainstObjectStorage() {
        Configuration c = ObjectStoreStateConfig.buildConfig("s3://bucket/mat");
        assertEquals(true, c.get(StateChangelogOptions.ENABLE_STATE_CHANGE_LOG));
        assertEquals("filesystem", c.getString("state.backend.changelog.storage", null));
        assertEquals("s3://bucket/mat/changelog", c.getString("dstl.dfs.base-path", null));
    }
}
