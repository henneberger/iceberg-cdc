package io.mat.source;

import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.iceberg.ChangelogOperation;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public final class IcebergCdcSplitSerializer implements SimpleVersionedSerializer<IcebergCdcSplit> {
    public static final IcebergCdcSplitSerializer INSTANCE = new IcebergCdcSplitSerializer();
    private static final int VERSION = 5;

    @Override public int getVersion() { return VERSION; }

    @Override public byte[] serialize(IcebergCdcSplit s) throws IOException {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        try (DataOutputStream out = new DataOutputStream(baos)) {
            out.writeUTF(s.splitId());
            out.writeLong(s.commitSnapshotId());
            out.writeInt(s.changeOrdinal());
            out.writeUTF(s.operation().name());
            out.writeUTF(s.dataFilePath());
            out.writeLong(s.fileSizeBytes());
            out.writeLong(s.startOffset());
            out.writeLong(s.lengthBytes());
            out.writeLong(s.dataSequenceNumber());
            out.writeLong(s.firstRowId());
            out.writeLong(s.commitTimestampMillis());
            out.writeInt(s.partitionSpecId());
            out.writeBoolean(s.deletesSelectRows());
            out.writeInt(s.deletes().size());
            for (IcebergCdcSplit.DeleteFileRef d : s.deletes()) {
                out.writeUTF(d.path);
                out.writeLong(d.fileSizeBytes);
                out.writeLong(d.sequenceNumber);
                out.writeUTF(d.content);
                out.writeLong(d.contentOffset);
                out.writeLong(d.contentSizeInBytes);
                out.writeInt(d.equalityFieldIds.length);
                for (int id : d.equalityFieldIds) out.writeInt(id);
            }
        }
        return baos.toByteArray();
    }

    @Override public IcebergCdcSplit deserialize(int version, byte[] bytes) throws IOException {
        if (version != VERSION) throw new IOException("unsupported split version " + version);
        try (DataInputStream in = new DataInputStream(new ByteArrayInputStream(bytes))) {
            String splitId = in.readUTF();
            long snapId = in.readLong();
            int ord = in.readInt();
            ChangelogOperation op = ChangelogOperation.valueOf(in.readUTF());
            String path = in.readUTF();
            long size = in.readLong();
            long start = in.readLong();
            long len = in.readLong();
            long dataSeq = in.readLong();
            long firstRowId = in.readLong();
            long commitTs = in.readLong();
            int specId = in.readInt();
            boolean deletesSelectRows = in.readBoolean();
            int n = in.readInt();
            List<IcebergCdcSplit.DeleteFileRef> deletes = new ArrayList<>(n);
            for (int i = 0; i < n; i++) {
                String dPath = in.readUTF();
                long dSize = in.readLong();
                long dSeq = in.readLong();
                String dContent = in.readUTF();
                long dOff = in.readLong();
                long dContentSize = in.readLong();
                int eqLen = in.readInt();
                int[] eqIds = new int[eqLen];
                for (int j = 0; j < eqLen; j++) eqIds[j] = in.readInt();
                deletes.add(new IcebergCdcSplit.DeleteFileRef(
                        dPath, dSize, dSeq, dContent, dOff, dContentSize, eqIds));
            }
            return new IcebergCdcSplit(splitId, snapId, ord, op, path,
                    size, start, len, dataSeq, firstRowId, commitTs, specId,
                    deletes, deletesSelectRows);
        }
    }
}
