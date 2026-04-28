package dev.henneberger.sql;

import dev.henneberger.source.IcebergCdcMetadata;
import dev.henneberger.source.IcebergCdcSource;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.ProviderContext;
import org.apache.flink.table.connector.source.DataStreamScanProvider;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.connector.source.ScanTableSource;
import org.apache.flink.table.connector.source.abilities.SupportsProjectionPushDown;
import org.apache.flink.table.connector.source.abilities.SupportsReadingMetadata;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.types.RowKind;
import org.apache.iceberg.MetadataColumns;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.flink.CatalogLoader;
import org.apache.iceberg.flink.TableLoader;

import java.time.Duration;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Flink SQL DynamicTableSource exposing an Iceberg v3 table as a CDC stream.
 *
 * <p>Emits {@link RowData} with {@link RowKind#INSERT} or {@link RowKind#DELETE}
 * tags, derived from Iceberg's per-snapshot {@link org.apache.iceberg.ChangelogOperation}
 * (manually planned because {@link org.apache.iceberg.IncrementalChangelogScan}
 * doesn't yet support delete files). Each row is timestamped with the snapshot
 * commit wallclock; downstream watermarks are derived from
 * {@code WatermarkStrategy.forBoundedOutOfOrderness(allowed-lateness)}.
 */
public final class IcebergCdcDynamicTableSource
        implements ScanTableSource, SupportsProjectionPushDown, SupportsReadingMetadata {

    private RowType producedRowType;
    private Map<String, String> metadataAliases = Map.of();
    private final IcebergCdcDynamicTableSourceFactory.Options opts;
    private final IcebergCdcDynamicTableSourceFactory.CatalogFactory catalogFactory;

    public IcebergCdcDynamicTableSource(
            RowType producedRowType,
            IcebergCdcDynamicTableSourceFactory.Options opts,
            IcebergCdcDynamicTableSourceFactory.CatalogFactory catalogFactory) {
        this.producedRowType = producedRowType;
        this.opts = opts;
        this.catalogFactory = catalogFactory;
    }

    @Override public boolean supportsNestedProjection() { return false; }

    @Override
    public Map<String, DataType> listReadableMetadata() {
        Map<String, DataType> metadata = new LinkedHashMap<>();
        metadata.put(MetadataColumns.ROW_ID.name(), DataTypes.BIGINT());
        metadata.put(MetadataColumns.LAST_UPDATED_SEQUENCE_NUMBER.name(), DataTypes.BIGINT());
        metadata.put(MetadataColumns.COMMIT_SNAPSHOT_ID.name(), DataTypes.BIGINT());
        metadata.put(IcebergCdcMetadata.COMMIT_TIMESTAMP_NAME, DataTypes.TIMESTAMP_LTZ(3));
        metadata.put(MetadataColumns.CHANGE_ORDINAL.name(), DataTypes.INT());
        metadata.put(MetadataColumns.CHANGE_TYPE.name(), DataTypes.STRING());
        metadata.put(MetadataColumns.FILE_PATH.name(), DataTypes.STRING());
        metadata.put(MetadataColumns.ROW_POSITION.name(), DataTypes.BIGINT());
        metadata.put(MetadataColumns.SPEC_ID.name(), DataTypes.INT());
        return metadata;
    }

    @Override
    public void applyReadableMetadata(List<String> metadataKeys, DataType producedDataType) {
        this.producedRowType = (RowType) producedDataType.getLogicalType();

        RowType rowType = this.producedRowType;
        int metadataStart = rowType.getFieldCount() - metadataKeys.size();
        Map<String, String> aliases = new HashMap<>();
        for (int i = 0; i < metadataKeys.size(); i++) {
            aliases.put(rowType.getFieldNames().get(metadataStart + i), metadataKeys.get(i));
        }
        this.metadataAliases = aliases;
    }

    @Override
    public boolean supportsMetadataProjection() {
        return true;
    }

    @Override
    public void applyProjection(int[][] projectedFields, DataType producedDataType) {
        this.producedRowType = (RowType) producedDataType.getLogicalType();
    }

    @Override
    public ChangelogMode getChangelogMode() {
        return ChangelogMode.newBuilder()
                .addContainedKind(RowKind.INSERT)
                .addContainedKind(RowKind.UPDATE_BEFORE)
                .addContainedKind(RowKind.UPDATE_AFTER)
                .addContainedKind(RowKind.DELETE)
                .build();
    }

    @Override
    public ScanRuntimeProvider getScanRuntimeProvider(ScanContext runtimeProviderContext) {
        RowType effectiveRowType = producedRowType;
        InternalTypeInfo<RowData> rowTypeInfo = InternalTypeInfo.of(effectiveRowType);
        IcebergCdcDynamicTableSourceFactory.Options o = opts;

        return new DataStreamScanProvider() {
            @Override
            public DataStream<RowData> produceDataStream(
                    ProviderContext providerContext, StreamExecutionEnvironment env) {

                CatalogLoader catalogLoader = buildCatalogLoader(o);
                TableLoader probe = TableLoader.fromCatalog(
                        catalogLoader, org.apache.iceberg.catalog.TableIdentifier.parse(o.tableId));
                Table table;
                try {
                    probe.open();
                    table = probe.loadTable();
                } finally {
                    try { probe.close(); } catch (Exception ignored) {}
                }

                // Build the Iceberg read schema from the planner-produced row type — not
                // table.schema() — so metadata columns and projection pushdown reach the reader.
                Schema schema = ddlRowTypeToIcebergSchema(
                        effectiveRowType, table.schema(), metadataAliases);

                IcebergCdcSource source = new IcebergCdcSource(
                        new SerializableTableSupplier(catalogLoader, o.tableId),
                        schema, o.discoveryInterval, o.bounded, null,
                        o.splitAssignmentMode, o.startupMode);

                WatermarkStrategy<RowData> wm = WatermarkStrategy
                        .<RowData>forBoundedOutOfOrderness(o.allowedLateness);

                return env.fromSource(source, wm, "iceberg-cdc-source", rowTypeInfo);
            }

            @Override public boolean isBounded() { return o.bounded; }
        };
    }

    /**
     * Convert the SQL DDL {@link RowType} to an Iceberg {@link Schema} suitable for the
     * reader. Looks each column up in the physical table schema by NAME so user-declared
     * lineage columns ({@code _row_id}, {@code _last_updated_sequence_number}) — which
     * are NOT in {@code table.schema()} — get the right v3 metadata column field IDs.
     */
    private static Schema ddlRowTypeToIcebergSchema(
            RowType ddl, Schema physical, Map<String, String> metadataAliases) {
        java.util.List<org.apache.iceberg.types.Types.NestedField> fields = new java.util.ArrayList<>();
        for (int i = 0; i < ddl.getFieldCount(); i++) {
            String name = ddl.getFieldNames().get(i);
            String metadataKey = metadataAliases.get(name);
            if (metadataKey != null) {
                addCompatibleField(fields, name, ddl.getTypeAt(i), metadataField(metadataKey));
                continue;
            }
            org.apache.iceberg.types.Types.NestedField fromTable = physical.findField(name);
            if (fromTable != null) {
                addCompatibleField(fields, name, ddl.getTypeAt(i), fromTable);
                continue;
            }
            // Iceberg CDC metadata columns: use Iceberg's well-known field IDs.
            org.apache.iceberg.types.Types.NestedField metadataField = metadataFieldOrNull(name);
            if (metadataField != null) {
                addCompatibleField(fields, name, ddl.getTypeAt(i), metadataField);
                continue;
            }
            throw new IllegalArgumentException(
                    "DDL column '" + name + "' has no matching physical column or v3 metadata column");
        }
        return new Schema(fields);
    }

    private static void addCompatibleField(
            java.util.List<org.apache.iceberg.types.Types.NestedField> fields,
            String ddlName,
            LogicalType ddlType,
            org.apache.iceberg.types.Types.NestedField icebergField) {
        LogicalType expected = org.apache.iceberg.flink.FlinkSchemaUtil.convert(icebergField.type());
        if (!ddlType.copy(true).equals(expected.copy(true))
                && !isCompatibleTimestampPrecision(ddlType, expected)) {
            throw new ValidationException(
                    "DDL column '" + ddlName + "' has type " + ddlType
                            + " but Iceberg field '" + icebergField.name()
                            + "' has type " + expected);
        }
        fields.add(icebergField);
    }

    private static boolean isCompatibleTimestampPrecision(LogicalType ddlType, LogicalType expected) {
        if (ddlType.getTypeRoot() != expected.getTypeRoot()) {
            return false;
        }
        int ddlPrecision = timestampPrecision(ddlType);
        int expectedPrecision = timestampPrecision(expected);
        return ddlPrecision >= 0
                && expectedPrecision >= 0
                && ddlPrecision <= expectedPrecision;
    }

    private static int timestampPrecision(LogicalType type) {
        switch (type.getTypeRoot()) {
            case TIMESTAMP_WITHOUT_TIME_ZONE:
                return ((org.apache.flink.table.types.logical.TimestampType) type).getPrecision();
            case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
                return ((org.apache.flink.table.types.logical.LocalZonedTimestampType) type).getPrecision();
            default:
                return -1;
        }
    }

    private static org.apache.iceberg.types.Types.NestedField metadataField(String key) {
        org.apache.iceberg.types.Types.NestedField field = metadataFieldOrNull(key);
        if (field != null) return field;
        throw new IllegalArgumentException("Unsupported readable metadata key '" + key + "'");
    }

    private static org.apache.iceberg.types.Types.NestedField metadataFieldOrNull(String key) {
        if (MetadataColumns.ROW_ID.name().equals(key)) return MetadataColumns.ROW_ID;
        if (MetadataColumns.LAST_UPDATED_SEQUENCE_NUMBER.name().equals(key)) {
            return MetadataColumns.LAST_UPDATED_SEQUENCE_NUMBER;
        }
        if (MetadataColumns.COMMIT_SNAPSHOT_ID.name().equals(key)) return MetadataColumns.COMMIT_SNAPSHOT_ID;
        if (IcebergCdcMetadata.COMMIT_TIMESTAMP_NAME.equals(key)) return IcebergCdcMetadata.COMMIT_TIMESTAMP;
        if (MetadataColumns.CHANGE_ORDINAL.name().equals(key)) return MetadataColumns.CHANGE_ORDINAL;
        if (MetadataColumns.CHANGE_TYPE.name().equals(key)) return MetadataColumns.CHANGE_TYPE;
        if (MetadataColumns.FILE_PATH.name().equals(key)) return MetadataColumns.FILE_PATH;
        if (MetadataColumns.ROW_POSITION.name().equals(key)) return MetadataColumns.ROW_POSITION;
        if (MetadataColumns.SPEC_ID.name().equals(key)) return MetadataColumns.SPEC_ID;
        return null;
    }

    private static CatalogLoader buildCatalogLoader(IcebergCdcDynamicTableSourceFactory.Options o) {
        Map<String, String> props = new HashMap<>();
        props.put("catalog-impl", o.catalogImpl);
        props.put("warehouse", o.warehouse);
        if (o.catalogUri != null) props.put("uri", o.catalogUri);
        if (o.region != null) props.put("client.region", o.region);
        if (o.catalogProps != null) props.putAll(o.catalogProps);
        org.apache.hadoop.conf.Configuration hadoopConf = new org.apache.hadoop.conf.Configuration();
        return CatalogLoader.custom("mat", props, hadoopConf, o.catalogImpl);
    }

    /** Serializable supplier used by the source's enumerator/reader. */
    private static final class SerializableTableSupplier
            implements dev.henneberger.source.IcebergCdcEnumerator.TableSupplier {
        private static final long serialVersionUID = 1L;
        private final CatalogLoader catalogLoader;
        private final String tableId;
        SerializableTableSupplier(CatalogLoader catalogLoader, String tableId) {
            this.catalogLoader = catalogLoader;
            this.tableId = tableId;
        }
        @Override public Table get() {
            TableLoader tl = TableLoader.fromCatalog(catalogLoader,
                    org.apache.iceberg.catalog.TableIdentifier.parse(tableId));
            try {
                tl.open();
                return tl.loadTable();
            } finally {
                try { tl.close(); } catch (Exception ignored) {}
            }
        }
    }

    @Override
    public DynamicTableSource copy() {
        IcebergCdcDynamicTableSource c = new IcebergCdcDynamicTableSource(producedRowType, opts, catalogFactory);
        c.metadataAliases = this.metadataAliases;
        return c;
    }

    @Override public String asSummaryString() { return "IcebergCdc[" + opts.tableId + "]"; }
}
