package dev.henneberger.source;

import org.apache.flink.core.io.SimpleVersionedSerializer;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public final class IcebergCdcEnumStateSerializer
        implements SimpleVersionedSerializer<IcebergCdcEnumState> {
    public static final IcebergCdcEnumStateSerializer INSTANCE = new IcebergCdcEnumStateSerializer();
    private static final int VERSION = 4;
    private static final int VERSION_WITHOUT_STARTUP_FLAG = 3;
    private final IcebergCdcSplitSerializer splitSer = IcebergCdcSplitSerializer.INSTANCE;

    @Override public int getVersion() { return VERSION; }

    @Override public byte[] serialize(IcebergCdcEnumState s) throws IOException {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        try (DataOutputStream out = new DataOutputStream(baos)) {
            out.writeLong(s.lastConsumedSnapshotId);
            out.writeBoolean(s.startupInitialized);
            out.writeInt(s.pendingSplits.size());
            for (IcebergCdcSplit split : s.pendingSplits) {
                byte[] b = splitSer.serialize(split);
                out.writeInt(b.length);
                out.write(b);
            }
        }
        return baos.toByteArray();
    }

    @Override public IcebergCdcEnumState deserialize(int version, byte[] bytes) throws IOException {
        if (version != VERSION && version != VERSION_WITHOUT_STARTUP_FLAG) {
            throw new IOException("unsupported state version " + version);
        }
        try (DataInputStream in = new DataInputStream(new ByteArrayInputStream(bytes))) {
            long bookmark = in.readLong();
            boolean startupInitialized = version == VERSION ? in.readBoolean() : true;
            int n = in.readInt();
            List<IcebergCdcSplit> pending = new ArrayList<>(n);
            for (int i = 0; i < n; i++) {
                int len = in.readInt();
                byte[] b = new byte[len];
                in.readFully(b);
                pending.add(splitSer.deserialize(splitSer.getVersion(), b));
            }
            return new IcebergCdcEnumState(bookmark, pending, startupInitialized);
        }
    }
}
