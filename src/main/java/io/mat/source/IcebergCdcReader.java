package io.mat.source;

import org.apache.flink.api.connector.source.SourceReaderContext;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.base.source.reader.RecordEmitter;
import org.apache.flink.connector.base.source.reader.SingleThreadMultiplexSourceReaderBase;
import org.apache.flink.table.data.RowData;
import org.apache.iceberg.Schema;

import java.util.Map;

/**
 * SourceReader for {@link IcebergCdcSplit}. Each emitted row carries the
 * snapshot commit timestamp as its event time, so a downstream
 * {@code WatermarkStrategy.forBoundedOutOfOrderness(...)} produces correct
 * watermarks for CDC streams (where there is no per-row event time — the
 * "time" of a delete is when it was committed).
 */
public final class IcebergCdcReader
        extends SingleThreadMultiplexSourceReaderBase<TimestampedRow, RowData,
        IcebergCdcSplit, IcebergCdcSplitState> {

    private final SourceReaderContext ctx;

    public IcebergCdcReader(
            IcebergCdcEnumerator.TableSupplier tableSupplier,
            Schema requestedSchema,
            SourceReaderContext ctx,
            Configuration config) {
        super(
                () -> new IcebergCdcSplitReader(tableSupplier, requestedSchema, ctx),
                (RecordEmitter<TimestampedRow, RowData, IcebergCdcSplitState>)
                        (record, output, splitState) -> {
                            output.collect(record.row, record.timestampMillis);
                            splitState.markEmittedThrough(record.nextRowPosition);
                        },
                config,
                ctx);
        this.ctx = ctx;
    }

    @Override public void start() { ctx.sendSplitRequest(); }

    @Override
    protected void onSplitFinished(Map<String, IcebergCdcSplitState> finishedSplits) {
        ctx.sendSplitRequest();
    }

    @Override
    protected IcebergCdcSplitState initializedState(IcebergCdcSplit split) {
        return new IcebergCdcSplitState(split);
    }

    @Override
    protected IcebergCdcSplit toSplitType(String splitId, IcebergCdcSplitState splitState) {
        return splitState.toSplit();
    }
}
