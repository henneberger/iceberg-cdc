package dev.henneberger.source;

import org.apache.flink.table.data.RowData;

/** Internal envelope: a {@link RowData} plus the snapshot commit timestamp to use as event-time. */
public final class TimestampedRow {
    public final RowData row;
    public final long timestampMillis;
    public final long nextRowPosition;

    public TimestampedRow(RowData row, long timestampMillis, long nextRowPosition) {
        this.row = row;
        this.timestampMillis = timestampMillis;
        this.nextRowPosition = nextRowPosition;
    }
}
