package io.mat.source;

import org.apache.flink.api.connector.source.ReaderInfo;
import org.apache.flink.api.connector.source.SourceEvent;
import org.apache.flink.api.connector.source.SplitEnumeratorContext;
import org.apache.flink.api.connector.source.SplitsAssignment;
import org.apache.flink.metrics.groups.SplitEnumeratorMetricGroup;
import org.apache.iceberg.ChangelogOperation;
import org.junit.jupiter.api.Test;

import javax.annotation.Nullable;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.function.BiConsumer;
import java.util.function.Consumer;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Unit-test the enumerator's failure recovery path: when a reader fails mid-split, Flink
 * calls {@code addSplitsBack(splits, subtaskId)}. The enumerator must restore them to its
 * pending queue so they get re-assigned, and snapshotting in that state must round-trip.
 */
class EnumeratorSplitRecoveryTest {

    @Test
    void splitsBackOnReaderFailureRestorePendingQueue() throws Exception {
        StubContext ctx = new StubContext();
        IcebergCdcEnumerator enumerator = new IcebergCdcEnumerator(
                ctx, () -> { throw new UnsupportedOperationException("not used"); },
                IcebergCdcEnumState.empty(), Duration.ofSeconds(30), false);

        // Synthesize 3 splits as if they had been assigned to subtask 0, then a crash
        // returns them to the enumerator.
        IcebergCdcSplit s1 = makeSplit("s1", "/path/A.parquet");
        IcebergCdcSplit s2 = makeSplit("s2", "/path/B.parquet");
        IcebergCdcSplit s3 = makeSplit("s3", "/path/C.parquet");
        enumerator.addSplitsBack(List.of(s1, s2, s3), 0);

        // Snapshotting now must include those splits so a savepoint preserves the work.
        IcebergCdcEnumState saved = enumerator.snapshotState(42L);
        assertEquals(3, saved.pendingSplits.size(),
                "splits added back must show up in checkpointed state");

        // Restore to a fresh enumerator: the pending queue must be re-populated.
        StubContext ctx2 = new StubContext();
        IcebergCdcEnumerator restored = new IcebergCdcEnumerator(
                ctx2, () -> { throw new UnsupportedOperationException(); },
                saved, Duration.ofSeconds(30), true);

        // Ask for splits as a fresh subtask. With one reader at parallelism=1, all three
        // splits hash to subtask 0 and should be assignable.
        ctx2.parallelism = 1;
        ctx2.registeredReaders.put(0, new ReaderInfo(0, "localhost"));
        // Drain via repeated handleSplitRequest calls.
        List<String> assigned = new ArrayList<>();
        for (int i = 0; i < 5; i++) {
            ctx2.lastAssignedSplitId = null;
            restored.handleSplitRequest(0, null);
            if (ctx2.lastAssignedSplitId != null) assigned.add(ctx2.lastAssignedSplitId);
        }
        // Exactly the 3 splits should come out in original order. Reversing these after
        // a reader failure can reorder INSERT/DELETE events for the same data file.
        assertEquals(3, assigned.size(),
                "all 3 recovered splits assigned via handleSplitRequest");
        assertEquals(List.of("s1", "s2", "s3"), assigned);
    }

    @Test
    void orderedModeRoutesAllSplitsToSubtaskZero() {
        StubContext ctx = new StubContext();
        ctx.parallelism = 2;
        ctx.registeredReaders.put(0, new ReaderInfo(0, "localhost"));
        ctx.registeredReaders.put(1, new ReaderInfo(1, "localhost"));
        IcebergCdcEnumerator enumerator = new IcebergCdcEnumerator(
                ctx, () -> { throw new UnsupportedOperationException("not used"); },
                IcebergCdcEnumState.empty(), Duration.ofSeconds(30), false,
                SplitAssignmentMode.ORDERED, StartupMode.EARLIEST);

        IcebergCdcSplit split = makeSplit("ordered", "/path/that-may-hash-elsewhere.parquet");
        enumerator.addSplitsBack(List.of(split), 0);

        enumerator.handleSplitRequest(1, null);
        assertNull(ctx.lastAssignedSplitId,
                "ordered mode must not assign CDC splits to non-zero subtasks");

        enumerator.handleSplitRequest(0, null);
        assertEquals("ordered", ctx.lastAssignedSplitId);
    }

    private static IcebergCdcSplit makeSplit(String splitId, String path) {
        return new IcebergCdcSplit(splitId, 1L, 0, ChangelogOperation.INSERT,
                path, 100L, 0L, 100L, 1L, 0L, 0, List.of());
    }

    /** Minimal SplitEnumeratorContext stub. */
    private static final class StubContext implements SplitEnumeratorContext<IcebergCdcSplit> {
        int parallelism = 1;
        final Map<Integer, ReaderInfo> registeredReaders = new HashMap<>();
        String lastAssignedSplitId;
        boolean noMoreSignaled;

        @Override public SplitEnumeratorMetricGroup metricGroup() { return null; }
        @Override public void sendEventToSourceReader(int subtaskId, SourceEvent event) {}
        @Override public void sendEventToSourceReader(int subtaskId, int attemptNumber, SourceEvent event) {}
        @Override public int currentParallelism() { return parallelism; }
        @Override public Map<Integer, ReaderInfo> registeredReaders() { return registeredReaders; }
        @Override public Map<Integer, Map<Integer, ReaderInfo>> registeredReadersOfAttempts() { return Map.of(); }

        @Override
        public void assignSplits(SplitsAssignment<IcebergCdcSplit> newSplitAssignments) {
            for (Map.Entry<Integer, List<IcebergCdcSplit>> e : newSplitAssignments.assignment().entrySet()) {
                if (!e.getValue().isEmpty()) lastAssignedSplitId = e.getValue().get(0).splitId();
            }
        }

        @Override
        public void assignSplit(IcebergCdcSplit split, int subtask) {
            lastAssignedSplitId = split.splitId();
        }

        @Override public void signalNoMoreSplits(int subtask) { noMoreSignaled = true; }

        @Override
        public <T> void callAsync(Callable<T> callable, BiConsumer<T, Throwable> handler) {}

        @Override
        public <T> void callAsync(Callable<T> callable, BiConsumer<T, Throwable> handler,
                                  long initialDelayMillis, long periodMillis) {}

        @Override public void runInCoordinatorThread(Runnable runnable) { runnable.run(); }

        @Override public void setIsProcessingBacklog(boolean b) {}
    }

}
