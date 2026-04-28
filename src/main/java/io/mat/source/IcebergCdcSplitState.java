package io.mat.source;

/**
 * Mutable reader-side state for one split.
 *
 * <p>The checkpointed split stores the next raw data-file row position to scan. This
 * avoids replaying already-emitted rows from a large split after a checkpoint restore.
 */
final class IcebergCdcSplitState {
    private final IcebergCdcSplit split;
    private long nextRowPosition;

    IcebergCdcSplitState(IcebergCdcSplit split) {
        this.split = split;
        this.nextRowPosition = split.startOffset();
    }

    void markEmittedThrough(long nextRowPosition) {
        this.nextRowPosition = Math.max(this.nextRowPosition, nextRowPosition);
    }

    IcebergCdcSplit toSplit() {
        return split.withStartOffset(nextRowPosition);
    }
}
