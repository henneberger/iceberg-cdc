package io.mat.source;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

/**
 * Checkpointed enumerator state.
 *
 * <p>{@code lastConsumedSnapshotId} is the high-water snapshot whose splits have been
 * <em>fully enqueued</em> into {@code pendingSplits} (or already drained). Discovery
 * resumes at the snapshot AFTER this one. {@code -1} means "first run, start from
 * the table's earliest visible snapshot".
 *
 * <p>{@code pendingSplits} carries any splits the enumerator has discovered but
 * has not yet handed to a reader (and the reader hasn't durably acked via its own
 * checkpoint state). On restart, we re-emit them so no work is lost.
 *
 * <p>Fixes the bug where the enumerator advanced its bookmark immediately after
 * discovery: a checkpoint taken in that window would then restart with an
 * advanced bookmark but no pending queue, dropping every yet-unassigned split.
 */
public final class IcebergCdcEnumState implements Serializable {
    private static final long serialVersionUID = 3L;

    public final long lastConsumedSnapshotId;
    public final List<IcebergCdcSplit> pendingSplits;
    public final boolean startupInitialized;

    public IcebergCdcEnumState(long lastConsumedSnapshotId, List<IcebergCdcSplit> pendingSplits) {
        this(lastConsumedSnapshotId, pendingSplits, true);
    }

    public IcebergCdcEnumState(long lastConsumedSnapshotId, List<IcebergCdcSplit> pendingSplits,
                               boolean startupInitialized) {
        this.lastConsumedSnapshotId = lastConsumedSnapshotId;
        this.pendingSplits = pendingSplits == null ? new ArrayList<>() : pendingSplits;
        this.startupInitialized = startupInitialized;
    }

    public IcebergCdcEnumState(long lastConsumedSnapshotId) {
        this(lastConsumedSnapshotId, new ArrayList<>());
    }

    public static IcebergCdcEnumState empty() {
        return new IcebergCdcEnumState(-1L, new ArrayList<>(), false);
    }
}
