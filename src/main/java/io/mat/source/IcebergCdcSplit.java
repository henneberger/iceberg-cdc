package io.mat.source;

import org.apache.flink.api.connector.source.SourceSplit;
import org.apache.iceberg.ChangelogOperation;

import java.io.Serializable;
import java.util.List;

/**
 * One Iceberg {@link org.apache.iceberg.ChangelogScanTask} wrapped as a Flink split.
 *
 * <p>Carries everything the SplitReader needs to read the data file and apply
 * the right delete files, without re-planning the scan: the data file path,
 * the delete file paths (positional / equality / v3 deletion vectors), and
 * the {@link ChangelogOperation} that determines the emitted {@code RowKind}.
 */
public final class IcebergCdcSplit implements SourceSplit, Serializable {

    private static final long serialVersionUID = 1L;

    private final String splitId;
    private final long commitSnapshotId;
    private final int changeOrdinal;
    private final ChangelogOperation operation;
    private final String dataFilePath;
    private final long fileSizeBytes;
    private final long startOffset;
    private final long lengthBytes;
    /** Iceberg sequence number of the data file. Used for v3 row-lineage stamping. */
    private final long dataSequenceNumber;
    /** First Iceberg v3 row id for this data file, or -1 when unavailable. */
    private final long firstRowId;
    /** Delete files this split's data file is subject to. */
    private final List<DeleteFileRef> deletes;
    /**
     * True when attached delete files identify the rows to emit (CDC delete-file
     * splits). False when attached delete files identify rows to suppress (insert
     * splits and removed-file splits that should emit only rows live before removal).
     */
    private final boolean deletesSelectRows;

    /** Snapshot commit wallclock millis — used as the event timestamp for emitted rows. */
    private final long commitTimestampMillis;
    /** Iceberg PartitionSpec id of the data file. Reader resolves the actual spec via Table.specs(). */
    private final int partitionSpecId;

    public IcebergCdcSplit(String splitId, long commitSnapshotId, int changeOrdinal,
                           ChangelogOperation operation, String dataFilePath,
                           long fileSizeBytes, long startOffset, long lengthBytes,
                           long dataSequenceNumber, long commitTimestampMillis,
                           int partitionSpecId,
                           List<DeleteFileRef> deletes) {
        this(splitId, commitSnapshotId, changeOrdinal, operation, dataFilePath,
                fileSizeBytes, startOffset, lengthBytes, dataSequenceNumber, -1L,
                commitTimestampMillis, partitionSpecId, deletes,
                operation == ChangelogOperation.DELETE);
    }

    public IcebergCdcSplit(String splitId, long commitSnapshotId, int changeOrdinal,
                           ChangelogOperation operation, String dataFilePath,
                           long fileSizeBytes, long startOffset, long lengthBytes,
                           long dataSequenceNumber, long firstRowId,
                           long commitTimestampMillis, int partitionSpecId,
                           List<DeleteFileRef> deletes, boolean deletesSelectRows) {
        this.splitId = splitId;
        this.commitSnapshotId = commitSnapshotId;
        this.changeOrdinal = changeOrdinal;
        this.operation = operation;
        this.dataFilePath = dataFilePath;
        this.fileSizeBytes = fileSizeBytes;
        this.startOffset = startOffset;
        this.lengthBytes = lengthBytes;
        this.dataSequenceNumber = dataSequenceNumber;
        this.firstRowId = firstRowId;
        this.commitTimestampMillis = commitTimestampMillis;
        this.partitionSpecId = partitionSpecId;
        this.deletes = deletes == null ? List.of() : deletes;
        this.deletesSelectRows = deletesSelectRows;
    }

    public long commitTimestampMillis() { return commitTimestampMillis; }
    public int partitionSpecId() { return partitionSpecId; }

    @Override public String splitId() { return splitId; }
    public long commitSnapshotId() { return commitSnapshotId; }
    public int changeOrdinal() { return changeOrdinal; }
    public ChangelogOperation operation() { return operation; }
    public String dataFilePath() { return dataFilePath; }
    public long fileSizeBytes() { return fileSizeBytes; }
    public long startOffset() { return startOffset; }
    public long lengthBytes() { return lengthBytes; }
    public long dataSequenceNumber() { return dataSequenceNumber; }
    public long firstRowId() { return firstRowId; }
    public List<DeleteFileRef> deletes() { return deletes; }
    public boolean deletesSelectRows() { return deletesSelectRows; }

    public IcebergCdcSplit withStartOffset(long newStartOffset) {
        return new IcebergCdcSplit(splitId, commitSnapshotId, changeOrdinal, operation,
                dataFilePath, fileSizeBytes, newStartOffset, lengthBytes, dataSequenceNumber,
                firstRowId, commitTimestampMillis, partitionSpecId, deletes, deletesSelectRows);
    }

    @Override public String toString() {
        return "IcebergCdcSplit{" + splitId + ",op=" + operation
                + ",snap=" + commitSnapshotId + ",ord=" + changeOrdinal
                + ",deletes=" + deletes.size()
                + ",deletesSelectRows=" + deletesSelectRows + "}";
    }

    /** A delete file reference that can be deserialized into an {@link org.apache.iceberg.DeleteFile} on the reader side. */
    public static final class DeleteFileRef implements Serializable {
        private static final long serialVersionUID = 1L;
        public final String path;
        public final long fileSizeBytes;
        public final long sequenceNumber;
        /** "EQUALITY_DELETES", "POSITION_DELETES", or "DELETION_VECTOR" */
        public final String content;
        /** Position in the puffin file for DV; -1 if not a DV. */
        public final long contentOffset;
        public final long contentSizeInBytes;
        /** Equality-key field IDs for EQUALITY_DELETES; empty for positional / DV. */
        public final int[] equalityFieldIds;

        public DeleteFileRef(String path, long fileSizeBytes, long sequenceNumber,
                             String content, long contentOffset, long contentSizeInBytes,
                             int[] equalityFieldIds) {
            this.path = path;
            this.fileSizeBytes = fileSizeBytes;
            this.sequenceNumber = sequenceNumber;
            this.content = content;
            this.contentOffset = contentOffset;
            this.contentSizeInBytes = contentSizeInBytes;
            this.equalityFieldIds = equalityFieldIds == null ? new int[0] : equalityFieldIds;
        }
    }
}
