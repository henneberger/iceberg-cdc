package dev.henneberger.source;

import org.apache.flink.api.connector.source.SplitEnumerator;
import org.apache.flink.api.connector.source.SplitEnumeratorContext;
import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.MetricGroup;
import org.apache.iceberg.ChangelogOperation;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.FileContent;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.Table;
import org.apache.iceberg.io.CloseableIterable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.io.IOException;
import java.time.Duration;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

/**
 * Walks Iceberg snapshot history forward, producing CDC splits.
 *
 * <p>Per snapshot S (operation ∉ {@link #COMPACTION_OPERATIONS}):
 * <ul>
 *   <li>Each {@code S.addedDataFiles()} → INSERT split. If S also adds a
 *       DV/positional delete that targets it, the matching positions are
 *       suppressed before emission (so the row never produces a phantom
 *       INSERT followed by an immediate DELETE for the same value).</li>
 *   <li>Each DV / positional delete in {@code S.addedDeleteFiles()} →
 *       DELETE split on the referenced data file (rows the DV marks deleted
 *       are read out and emitted as -D).</li>
 *   <li>Each EQUALITY delete in {@code S.addedDeleteFiles()} → one DELETE
 *       split per affected prior data file. The reader scans the data file,
 *       checks each row's equality-key columns against the equality-set
 *       loaded from the delete file, and emits matches as -D.</li>
 *   <li>Each {@code S.removedDataFiles()} → DELETE split for every row in
 *       the dropped file (e.g. overwrite without replacement).</li>
 * </ul>
 */
public final class IcebergCdcEnumerator
        implements SplitEnumerator<IcebergCdcSplit, IcebergCdcEnumState> {

    private static final Logger LOG = LoggerFactory.getLogger(IcebergCdcEnumerator.class);

    static final Set<String> COMPACTION_OPERATIONS = Set.of("replace");

    static final Set<String> COMPACTION_SUMMARY_KEYS = Set.of(
            "rewritten-files-count", "added-position-delete-files-rewrite",
            "data-files-rewrite", "compaction");

    private final SplitEnumeratorContext<IcebergCdcSplit> ctx;
    private final TableSupplier tableSupplier;
    private final Duration discoveryInterval;
    private final boolean bounded;
    private final SplitAssignmentMode splitAssignmentMode;
    private final StartupMode startupMode;

    private final Deque<IcebergCdcSplit> pending = new ArrayDeque<>();
    private long lastConsumedSnapshotId;
    private boolean firstDiscoveryDone = false;
    private boolean startupInitialized = false;
    private final Counter snapshotsDiscovered;
    private final Counter splitsDiscovered;
    private final Counter discoveryFailures;

    public IcebergCdcEnumerator(
            SplitEnumeratorContext<IcebergCdcSplit> ctx,
            TableSupplier tableSupplier,
            IcebergCdcEnumState restored,
            Duration discoveryInterval,
            boolean bounded) {
        this(ctx, tableSupplier, restored, discoveryInterval, bounded,
                SplitAssignmentMode.ORDERED, StartupMode.EARLIEST);
    }

    public IcebergCdcEnumerator(
            SplitEnumeratorContext<IcebergCdcSplit> ctx,
            TableSupplier tableSupplier,
            IcebergCdcEnumState restored,
            Duration discoveryInterval,
            boolean bounded,
            SplitAssignmentMode splitAssignmentMode,
            StartupMode startupMode) {
        this.ctx = ctx;
        this.tableSupplier = tableSupplier;
        this.discoveryInterval = discoveryInterval;
        this.bounded = bounded;
        this.splitAssignmentMode = splitAssignmentMode == null
                ? SplitAssignmentMode.ORDERED
                : splitAssignmentMode;
        this.startupMode = startupMode == null ? StartupMode.EARLIEST : startupMode;
        if (restored != null) {
            this.lastConsumedSnapshotId = restored.lastConsumedSnapshotId;
            this.startupInitialized = restored.startupInitialized;
            // Restore yet-to-be-assigned splits so we don't lose work across failover.
            this.pending.addAll(restored.pendingSplits);
        } else {
            this.lastConsumedSnapshotId = -1L;
        }
        MetricGroup metrics = ctx.metricGroup();
        if (metrics == null) {
            this.snapshotsDiscovered = null;
            this.splitsDiscovered = null;
            this.discoveryFailures = null;
        } else {
            this.snapshotsDiscovered = metrics.counter("snapshotsDiscovered");
            this.splitsDiscovered = metrics.counter("splitsDiscovered");
            this.discoveryFailures = metrics.counter("discoveryFailures");
            metrics.gauge("pendingSplits", () -> pending.size());
            metrics.gauge("lastConsumedSnapshotId", () -> lastConsumedSnapshotId);
        }
    }

    @Override
    public void start() {
        if (bounded) {
            ctx.callAsync(this::discoverNewSplits, this::handleDiscovered);
        } else {
            ctx.callAsync(this::discoverNewSplits, this::handleDiscovered,
                    0, discoveryInterval.toMillis());
        }
    }

    @Override
    public void handleSplitRequest(int subtaskId, @Nullable String requesterHostname) {
        // Affinity: pop the first split whose target subtask matches subtaskId. Walk the
        // ENTIRE queue (size frozen up front; do not compare against the shrinking deque)
        // so we never falsely signal noMoreSplits while a matching split is at the tail.
        int parallelism = Math.max(1, ctx.currentParallelism());
        int initialSize = pending.size();
        IcebergCdcSplit found = null;
        Deque<IcebergCdcSplit> deferred = new ArrayDeque<>();
        for (int i = 0; i < initialSize; i++) {
            IcebergCdcSplit s = pending.pollFirst();
            if (s == null) break;
            if (subtaskFor(s, parallelism) == subtaskId) { found = s; break; }
            deferred.addLast(s);
        }
        // Restore unmatched splits, preserving original order (head→tail).
        while (!deferred.isEmpty()) pending.addFirst(deferred.pollLast());

        if (found != null) {
            ctx.assignSplit(found, subtaskId);
        } else if (bounded && firstDiscoveryDone) {
            // No matching split for this subtask AND we won't discover more. Confirm there's
            // truly nothing left destined for this subtask before signalling.
            boolean anyForSubtask = false;
            for (IcebergCdcSplit s : pending) {
                if (subtaskFor(s, parallelism) == subtaskId) { anyForSubtask = true; break; }
            }
            if (!anyForSubtask) ctx.signalNoMoreSplits(subtaskId);
        }
    }

    /** In ordered mode all splits go through subtask 0; file-affinity mode hashes by path. */
    private int subtaskFor(IcebergCdcSplit s, int parallelism) {
        if (splitAssignmentMode == SplitAssignmentMode.ORDERED) {
            return 0;
        }
        int h = s.dataFilePath().hashCode();
        return Math.floorMod(h, parallelism);
    }

    @Override
    public void addSplitsBack(List<IcebergCdcSplit> splits, int subtaskId) {
        for (int i = splits.size() - 1; i >= 0; i--) {
            pending.addFirst(splits.get(i));
        }
    }

    @Override public void addReader(int subtaskId) {}

    @Override
    public IcebergCdcEnumState snapshotState(long checkpointId) {
        // Snapshot the pending queue too. Without this, a checkpoint taken after we
        // advance lastConsumedSnapshotId but before assigning all of that snapshot's
        // splits would lose those splits on restart.
        return new IcebergCdcEnumState(lastConsumedSnapshotId, new ArrayList<>(pending),
                startupInitialized);
    }

    @Override public void close() throws IOException {}

    // ----- discovery -----

    private DiscoveryResult discoverNewSplits() {
        Table table = tableSupplier.get();
        table.refresh();
        Snapshot current = table.currentSnapshot();
        if (!startupInitialized && startupMode == StartupMode.LATEST) {
            if (current == null) {
                return new DiscoveryResult(List.of(), true);
            }
            return new DiscoveryResult(
                    List.of(new PlannedSnapshot(current.snapshotId(), List.of())),
                    true);
        }
        if (current == null) return new DiscoveryResult(List.of(), true);

        // Build the chain (lastConsumedSnapshotId, current] in oldest-first order.
        // If we have a bookmark but it's no longer in the history (table was rolled back,
        // or expire-snapshots removed it), fail loud rather than silently replaying everything.
        List<Snapshot> chain = new ArrayList<>();
        Snapshot cursor = current;
        boolean foundBookmark = (lastConsumedSnapshotId == -1L);
        while (cursor != null) {
            if (cursor.snapshotId() == lastConsumedSnapshotId) {
                foundBookmark = true;
                break;
            }
            chain.add(0, cursor);
            Long parent = cursor.parentId();
            if (parent == null) break;
            cursor = table.snapshot(parent);
        }
        if (lastConsumedSnapshotId != -1L && !foundBookmark) {
            // Bookmark not in current snapshot history. Don't silently replay; surface it.
            throw new IllegalStateException(
                    "Bookmark snapshot " + lastConsumedSnapshotId + " is no longer in the "
                    + "table's snapshot history. The source likely missed snapshot expiry; "
                    + "drop checkpoint state and restart from a fresh bookmark to proceed.");
        }
        if (chain.isEmpty()) return new DiscoveryResult(List.of(), true);

        // Plan each snapshot. Pair planned splits with the snapshot id so the caller
        // can advance the bookmark only after that snapshot's splits are enqueued.
        List<PlannedSnapshot> planned = new ArrayList<>(chain.size());
        int changeOrdinal = 0;
        for (Snapshot snap : chain) {
            if (isMaintenance(snap)) {
                LOG.info("[mat-cdc] skipping maintenance snapshot {} op={}",
                        snap.snapshotId(), snap.operation());
                planned.add(new PlannedSnapshot(snap.snapshotId(), List.of()));
                continue;
            }
            planned.add(new PlannedSnapshot(snap.snapshotId(),
                    planSnapshot(table, snap, changeOrdinal++)));
        }
        return new DiscoveryResult(planned, true);
    }

    private List<IcebergCdcSplit> planSnapshot(Table table, Snapshot snap, int changeOrdinal) {
        List<IcebergCdcSplit> deletesOut = new ArrayList<>();
        List<IcebergCdcSplit> insertsOut = new ArrayList<>();
        List<DataFile> addedDataFiles = iterableToList(snap.addedDataFiles(table.io()));
        List<DataFile> removedDataFiles = iterableToList(snap.removedDataFiles(table.io()));
        Set<String> addedDataFilePaths = new java.util.HashSet<>();
        for (DataFile df : addedDataFiles) {
            addedDataFilePaths.add(df.path().toString());
        }

        // Bucket added delete files by type and target.
        Map<String, List<DeleteFile>> positionDeletesByDataFile = new LinkedHashMap<>();
        List<DeleteFile> equalityDeletes = new ArrayList<>();
        ParentSnapshotIndex parentIndex = null;

        for (DeleteFile dfile : snap.addedDeleteFiles(table.io())) {
            validateDeleteFileFormat(dfile);
            if (dfile.content() == FileContent.EQUALITY_DELETES) {
                equalityDeletes.add(dfile);
            } else {
                String ref = dfile.referencedDataFile();
                if (ref == null) {
                    if (dfile.content() != FileContent.POSITION_DELETES) {
                        throw new UnsupportedOperationException(
                                "Delete file " + dfile.path() + " has no referencedDataFile. "
                                        + "Only global POSITION_DELETES are supported without "
                                        + "a referenced data file.");
                    }
                    if (parentIndex == null) parentIndex = ParentSnapshotIndex.build(table, snap);
                    for (DataFile affected : findDataFilesAffectedByPositionDelete(parentIndex, dfile)) {
                        positionDeletesByDataFile
                                .computeIfAbsent(affected.path().toString(), k -> new ArrayList<>())
                                .add(dfile);
                    }
                    continue;
                }
                positionDeletesByDataFile.computeIfAbsent(ref, k -> new ArrayList<>()).add(dfile);
            }
        }

        // INSERT: every added data file. Apply same-snapshot DVs to suppress positions.
        for (DataFile df : addedDataFiles) {
            List<DeleteFile> applicable =
                    positionDeletesByDataFile.getOrDefault(df.path().toString(), List.of());
            insertsOut.add(buildSplit(snap, changeOrdinal, ChangelogOperation.INSERT,
                    df, applicable, false));
        }

        // DELETE: each newly added DV → emit the rows it marks deleted.
        for (Map.Entry<String, List<DeleteFile>> e : positionDeletesByDataFile.entrySet()) {
            if (addedDataFilePaths.contains(e.getKey())) {
                continue;
            }
            if (parentIndex == null) parentIndex = ParentSnapshotIndex.build(table, snap);
            DataFile target = parentIndex.file(e.getKey());
            if (target == null) {
                throw new IllegalStateException(
                        "Could not find live data file '" + e.getKey()
                                + "' referenced by positional delete in parent snapshot of "
                                + snap.snapshotId());
            }
            deletesOut.add(buildSplit(snap, changeOrdinal, ChangelogOperation.DELETE,
                    target, e.getValue(), true));
        }

        // DELETE: each newly added equality delete file → one split per affected prior data file.
        for (DeleteFile edf : equalityDeletes) {
            if (parentIndex == null) parentIndex = ParentSnapshotIndex.build(table, snap);
            for (DataFile affected : findDataFilesAffectedByEqualityDelete(parentIndex, edf)) {
                deletesOut.add(buildSplit(snap, changeOrdinal, ChangelogOperation.DELETE,
                        affected, List.of(edf), true));
            }
        }

        // DELETE: rows in fully-removed data files. Attach delete files already
        // active at the parent snapshot so the reader emits only rows that were
        // live immediately before this removal.
        for (DataFile df : removedDataFiles) {
            if (parentIndex == null) parentIndex = ParentSnapshotIndex.build(table, snap);
            deletesOut.add(buildSplit(snap, changeOrdinal, ChangelogOperation.DELETE,
                    df, activeDeletesForRemovedFile(parentIndex, snap, df), false));
        }

        List<IcebergCdcSplit> out = new ArrayList<>(deletesOut.size() + insertsOut.size());
        out.addAll(deletesOut);
        out.addAll(insertsOut);
        return out;
    }

    private static <T> List<T> iterableToList(Iterable<T> iterable) {
        List<T> out = new ArrayList<>();
        for (T item : iterable) {
            out.add(item);
        }
        return out;
    }

    /**
     * Find data files affected by a global positional delete file. Global position deletes
     * carry the target data file path in each delete row rather than in {@code referencedDataFile}.
     * Iceberg's delete loader will filter by path in the reader, so planning keeps the prior
     * live-file scan conservative, but the scan is shared per snapshot by {@link ParentSnapshotIndex}.
     */
    private static List<DataFile> findDataFilesAffectedByPositionDelete(ParentSnapshotIndex index,
                                                                        DeleteFile dfile) {
        Long delSeq = dfile.dataSequenceNumber();
        List<DataFile> out = new ArrayList<>();
        for (ParentFile parentFile : index.files()) {
            DataFile df = parentFile.file();
            Long fileSeq = df.dataSequenceNumber();
            if (delSeq != null && fileSeq != null && fileSeq >= delSeq) continue;
            if (!partitionsMatch(df, dfile)) continue;
            out.add(df);
        }
        return out;
    }

    private static List<DeleteFile> activeDeletesForRemovedFile(
            ParentSnapshotIndex index, Snapshot snap, DataFile removed) {
        if (snap.parentId() == null) return List.of();
        ParentFile parent = index.parentFile(removed.path().toString());
        if (parent == null) {
            throw new IllegalStateException(
                    "Removed data file '" + removed.path() + "' was not live in parent snapshot "
                            + snap.parentId() + " of snapshot " + snap.snapshotId());
        }
        return parent.deletes();
    }

    /**
     * Find data files that an equality delete may have affected: any data file with
     * sequence number {@code <} the delete's sequence number, in the same partition,
     * that is still alive at the snapshot the delete was added in.
     */
    private static List<DataFile> findDataFilesAffectedByEqualityDelete(
            ParentSnapshotIndex index, DeleteFile edf) {
        Long delSeq = edf.dataSequenceNumber();
        if (delSeq == null) {
            throw new IllegalStateException(
                    "Equality delete file " + edf.path()
                            + " has no data sequence number; cannot determine affected files");
        }

        List<DataFile> out = new ArrayList<>();
        for (ParentFile parentFile : index.files()) {
            DataFile df = parentFile.file();
            Long fileSeq = df.dataSequenceNumber();
            if (fileSeq == null || fileSeq >= delSeq) continue;
            if (!partitionsMatch(df, edf)) continue;
            out.add(df);
        }
        return out;
    }

    private static boolean partitionsMatch(DataFile df, DeleteFile edf) {
        // Equality deletes scope to a partition (or are global if specId == 0 unpartitioned).
        if (df.specId() != edf.specId()) return false;
        if (df.partition() == null && edf.partition() == null) return true;
        if (df.partition() == null || edf.partition() == null) return false;
        if (df.partition().size() != edf.partition().size()) return false;
        for (int i = 0; i < df.partition().size(); i++) {
            Object a = df.partition().get(i, Object.class);
            Object b = edf.partition().get(i, Object.class);
            if (a == null ? b != null : !a.equals(b)) return false;
        }
        return true;
    }

    private static void validateDeleteFileFormat(DeleteFile dfile) {
        if (dfile.content() == FileContent.POSITION_DELETES) {
            if (dfile.format() != FileFormat.PARQUET && dfile.format() != FileFormat.PUFFIN) {
                throw new UnsupportedOperationException(
                        "Iceberg CDC supports position deletes only in Parquet files or Puffin "
                                + "deletion vectors. Unsupported "
                                + "delete file " + dfile.path() + " has format " + dfile.format() + ".");
            }
            return;
        }
        if (dfile.content() == FileContent.EQUALITY_DELETES) {
            if (dfile.format() != FileFormat.PARQUET) {
                throw new UnsupportedOperationException(
                        "Iceberg CDC currently supports Parquet equality delete files only. "
                                + "Unsupported delete file " + dfile.path() + " has format "
                                + dfile.format() + ".");
            }
            return;
        }
        throw new UnsupportedOperationException(
                "Unsupported Iceberg delete file content " + dfile.content()
                        + " for file " + dfile.path());
    }

    private static IcebergCdcSplit buildSplit(Snapshot snap, int changeOrdinal,
                                              ChangelogOperation op, DataFile df,
                                              List<DeleteFile> deletes,
                                              boolean deletesSelectRows) {
        if (df.format() != FileFormat.PARQUET) {
            throw new UnsupportedOperationException(
                    "Iceberg CDC currently supports Parquet data files only. Unsupported file "
                            + df.path() + " has format " + df.format() + ".");
        }
        List<IcebergCdcSplit.DeleteFileRef> refs = new ArrayList<>(deletes.size());
        for (DeleteFile dfile : deletes) {
            validateDeleteFileFormat(dfile);
            int[] eqIds = new int[0];
            if (dfile.content() == FileContent.EQUALITY_DELETES && dfile.equalityFieldIds() != null) {
                eqIds = new int[dfile.equalityFieldIds().size()];
                for (int i = 0; i < eqIds.length; i++) {
                    eqIds[i] = dfile.equalityFieldIds().get(i);
                }
            }
            refs.add(new IcebergCdcSplit.DeleteFileRef(
                    dfile.path().toString(),
                    dfile.fileSizeInBytes(),
                    dfile.dataSequenceNumber() == null ? -1L : dfile.dataSequenceNumber(),
                    dfile.content().name(),
                    dfile.contentOffset() == null ? -1L : dfile.contentOffset(),
                    dfile.contentSizeInBytes() == null ? -1L : dfile.contentSizeInBytes(),
                    eqIds));
        }
        String splitId = snap.snapshotId() + ":" + changeOrdinal + ":" + op + ":" + UUID.randomUUID();
        long dataSeq = df.dataSequenceNumber() == null ? -1L : df.dataSequenceNumber();
        long firstRowId = df.firstRowId() == null ? -1L : df.firstRowId();
        return new IcebergCdcSplit(splitId, snap.snapshotId(), changeOrdinal, op,
                df.path().toString(), df.fileSizeInBytes(), 0L, df.fileSizeInBytes(),
                dataSeq, firstRowId, snap.timestampMillis(), df.specId(), refs,
                deletesSelectRows);
    }

    static boolean isMaintenance(Snapshot snap) {
        if (snap.operation() != null && COMPACTION_OPERATIONS.contains(snap.operation())) {
            return true;
        }
        if (snap.summary() != null) {
            for (String key : COMPACTION_SUMMARY_KEYS) {
                if (snap.summary().containsKey(key)) return true;
            }
        }
        return false;
    }

    private void handleDiscovered(DiscoveryResult result, Throwable err) {
        if (err != null) {
            // For unbounded streams, we tolerate transient discovery failures and retry.
            // For bounded reads (and for non-recoverable errors like bookmark drift),
            // propagate so the job fails fast rather than hanging forever.
            Throwable root = err;
            while (root.getCause() != null) root = root.getCause();
            boolean fatal = bounded
                    || root instanceof IllegalStateException
                    || root instanceof IllegalArgumentException
                    || root instanceof UnsupportedOperationException;
            if (discoveryFailures != null) discoveryFailures.inc();
            if (fatal) {
                LOG.error("Discovery failed; failing job", err);
                throw new RuntimeException("Iceberg CDC discovery failed", err);
            }
            LOG.warn("Discovery failed; will retry next tick", err);
            return;
        }
        // Advance the bookmark per snapshot, only AFTER its splits are durably in `pending`.
        // A subsequent snapshotState() will then carry both the new bookmark and the splits;
        // restart restores both, so no work is lost.
        for (PlannedSnapshot p : result.planned) {
            for (IcebergCdcSplit s : p.splits) pending.addLast(s);
            if (snapshotsDiscovered != null) snapshotsDiscovered.inc();
            if (splitsDiscovered != null) splitsDiscovered.inc(p.splits.size());
            lastConsumedSnapshotId = p.snapshotId;
        }
        startupInitialized = startupInitialized || result.startupInitialized;
        firstDiscoveryDone = true;

        // Drain pending using the same affinity rule as handleSplitRequest.
        if (!pending.isEmpty() && !ctx.registeredReaders().isEmpty()) {
            int parallelism = Math.max(1, ctx.currentParallelism());
            int registered = ctx.registeredReaders().size();
            Deque<IcebergCdcSplit> retry = new ArrayDeque<>();
            while (!pending.isEmpty()) {
                IcebergCdcSplit s = pending.pollFirst();
                int target = subtaskFor(s, parallelism);
                if (ctx.registeredReaders().containsKey(target)) {
                    ctx.assignSplit(s, target);
                } else {
                    retry.addLast(s);
                }
            }
            while (!retry.isEmpty()) pending.addLast(retry.pollFirst());
            // If we couldn't dispatch (no readers for those targets), keep pending
            // for the next handleSplitRequest cycle.
            if (parallelism == registered && pending.isEmpty() && bounded) {
                for (Integer subtask : ctx.registeredReaders().keySet()) {
                    ctx.signalNoMoreSplits(subtask);
                }
            }
        } else if (bounded && pending.isEmpty()) {
            for (Integer subtask : ctx.registeredReaders().keySet()) {
                ctx.signalNoMoreSplits(subtask);
            }
        }
    }

    /** Per-snapshot planning result; the enumerator commits the bookmark only after each. */
    private record PlannedSnapshot(long snapshotId, List<IcebergCdcSplit> splits) {}

    private record DiscoveryResult(List<PlannedSnapshot> planned, boolean startupInitialized) {}

    private record ParentFile(DataFile file, List<DeleteFile> deletes) {}

    private static final class ParentSnapshotIndex {
        private static final ParentSnapshotIndex EMPTY =
                new ParentSnapshotIndex(List.of(), Map.of());

        private final List<ParentFile> files;
        private final Map<String, ParentFile> byPath;

        private ParentSnapshotIndex(List<ParentFile> files, Map<String, ParentFile> byPath) {
            this.files = files;
            this.byPath = byPath;
        }

        static ParentSnapshotIndex build(Table table, Snapshot snap) {
            Long parentId = snap.parentId();
            if (parentId == null) return EMPTY;

            List<ParentFile> files = new ArrayList<>();
            Map<String, ParentFile> byPath = new LinkedHashMap<>();
            try (CloseableIterable<FileScanTask> tasks =
                         table.newScan().useSnapshot(parentId).planFiles()) {
                for (FileScanTask task : tasks) {
                    ParentFile parentFile = new ParentFile(
                            task.file(), new ArrayList<>(task.deletes()));
                    files.add(parentFile);
                    byPath.put(task.file().path().toString(), parentFile);
                }
            } catch (IOException e) {
                throw new RuntimeException(
                        "Failed to scan parent snapshot " + parentId
                                + " while planning CDC snapshot " + snap.snapshotId(), e);
            }
            return new ParentSnapshotIndex(files, byPath);
        }

        List<ParentFile> files() {
            return files;
        }

        DataFile file(String path) {
            ParentFile parentFile = byPath.get(path);
            return parentFile == null ? null : parentFile.file();
        }

        ParentFile parentFile(String path) {
            return byPath.get(path);
        }
    }

    @FunctionalInterface
    public interface TableSupplier extends java.io.Serializable { Table get(); }
}
