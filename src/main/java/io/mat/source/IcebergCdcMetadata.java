package io.mat.source;

import org.apache.iceberg.types.Types;

/** Connector-specific readable metadata fields that Iceberg does not expose directly. */
public final class IcebergCdcMetadata {
    public static final String COMMIT_TIMESTAMP_NAME = "_commit_timestamp";
    public static final int COMMIT_TIMESTAMP_FIELD_ID = 2147483538;
    public static final Types.NestedField COMMIT_TIMESTAMP = Types.NestedField.optional(
            COMMIT_TIMESTAMP_FIELD_ID,
            COMMIT_TIMESTAMP_NAME,
            Types.TimestampType.withZone());

    private IcebergCdcMetadata() {}
}
