package dev.henneberger.source;

import org.apache.flink.api.connector.source.Boundedness;
import org.apache.flink.api.connector.source.Source;
import org.apache.flink.api.connector.source.SourceReader;
import org.apache.flink.api.connector.source.SourceReaderContext;
import org.apache.flink.api.connector.source.SplitEnumerator;
import org.apache.flink.api.connector.source.SplitEnumeratorContext;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.table.data.RowData;
import org.apache.iceberg.Schema;

import java.time.Duration;

/**
 * FLIP-27 source that emits Iceberg v3 changelog as Flink {@link RowData} with
 * {@link org.apache.flink.types.RowKind} tags. Planning manually walks Iceberg
 * snapshot history because Iceberg's incremental changelog scan does not cover
 * delete-file CDC semantics; readers apply positional/equality deletes via
 * {@link org.apache.iceberg.data.BaseDeleteLoader}.
 */
public class IcebergCdcSource
        implements Source<RowData, IcebergCdcSplit, IcebergCdcEnumState> {

    private static final long serialVersionUID = 1L;

    private final IcebergCdcEnumerator.TableSupplier tableSupplier;
    private final Schema requestedSchema;
    private final Duration discoveryInterval;
    private final boolean bounded;
    private final SplitAssignmentMode splitAssignmentMode;
    private final StartupMode startupMode;
    /** Optional initial state — if set, takes effect on first createEnumerator call. */
    private final IcebergCdcEnumState initialState;

    public IcebergCdcSource(
            IcebergCdcEnumerator.TableSupplier tableSupplier,
            Schema requestedSchema,
            Duration discoveryInterval,
            boolean bounded) {
        this(tableSupplier, requestedSchema, discoveryInterval, bounded, null,
                SplitAssignmentMode.ORDERED, StartupMode.EARLIEST);
    }

    public IcebergCdcSource(
            IcebergCdcEnumerator.TableSupplier tableSupplier,
            Schema requestedSchema,
            Duration discoveryInterval,
            boolean bounded,
            IcebergCdcEnumState initialState) {
        this(tableSupplier, requestedSchema, discoveryInterval, bounded, initialState,
                SplitAssignmentMode.ORDERED, StartupMode.EARLIEST);
    }

    public IcebergCdcSource(
            IcebergCdcEnumerator.TableSupplier tableSupplier,
            Schema requestedSchema,
            Duration discoveryInterval,
            boolean bounded,
            IcebergCdcEnumState initialState,
            SplitAssignmentMode splitAssignmentMode,
            StartupMode startupMode) {
        this.tableSupplier = tableSupplier;
        this.requestedSchema = requestedSchema;
        this.discoveryInterval = discoveryInterval;
        this.bounded = bounded;
        this.initialState = initialState;
        this.splitAssignmentMode = splitAssignmentMode == null
                ? SplitAssignmentMode.ORDERED
                : splitAssignmentMode;
        this.startupMode = startupMode == null ? StartupMode.EARLIEST : startupMode;
    }

    @Override
    public Boundedness getBoundedness() {
        return bounded ? Boundedness.BOUNDED : Boundedness.CONTINUOUS_UNBOUNDED;
    }

    @Override
    public SourceReader<RowData, IcebergCdcSplit> createReader(SourceReaderContext readerContext) {
        return new IcebergCdcReader(tableSupplier, requestedSchema, readerContext, new Configuration());
    }

    @Override
    public SplitEnumerator<IcebergCdcSplit, IcebergCdcEnumState> createEnumerator(
            SplitEnumeratorContext<IcebergCdcSplit> enumContext) {
        IcebergCdcEnumState start = initialState == null ? IcebergCdcEnumState.empty() : initialState;
        return new IcebergCdcEnumerator(enumContext, tableSupplier, start,
                discoveryInterval, bounded, splitAssignmentMode, startupMode);
    }

    @Override
    public SplitEnumerator<IcebergCdcSplit, IcebergCdcEnumState> restoreEnumerator(
            SplitEnumeratorContext<IcebergCdcSplit> enumContext, IcebergCdcEnumState checkpoint) {
        return new IcebergCdcEnumerator(enumContext, tableSupplier, checkpoint,
                discoveryInterval, bounded, splitAssignmentMode, startupMode);
    }

    @Override
    public SimpleVersionedSerializer<IcebergCdcSplit> getSplitSerializer() {
        return IcebergCdcSplitSerializer.INSTANCE;
    }

    @Override
    public SimpleVersionedSerializer<IcebergCdcEnumState> getEnumeratorCheckpointSerializer() {
        return IcebergCdcEnumStateSerializer.INSTANCE;
    }
}
