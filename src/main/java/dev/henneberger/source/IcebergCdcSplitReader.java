package dev.henneberger.source;

import org.apache.flink.connector.base.source.reader.RecordsBySplits;
import org.apache.flink.connector.base.source.reader.RecordsWithSplitIds;
import org.apache.flink.connector.base.source.reader.splitreader.SplitReader;
import org.apache.flink.connector.base.source.reader.splitreader.SplitsAddition;
import org.apache.flink.connector.base.source.reader.splitreader.SplitsChange;
import org.apache.flink.api.connector.source.SourceReaderContext;
import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.table.data.RowData;
import org.apache.flink.types.RowKind;
import org.apache.iceberg.ChangelogOperation;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.FileMetadata;
import org.apache.iceberg.MetadataColumns;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.data.BaseDeleteLoader;
import org.apache.iceberg.deletes.PositionDeleteIndex;
import org.apache.iceberg.flink.data.FlinkParquetReaders;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.parquet.Parquet;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.util.StructLikeSet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Deque;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Reads one {@link IcebergCdcSplit} at a time and emits {@link TimestampedRow}.
 *
 * <p>Three delete-application modes:
 * <ul>
 *   <li><b>INSERT split</b>: read the data file. If a DV is attached (same-snapshot
 *       upsert with a delete on the new file), drop the DV-marked positions. Tag
 *       surviving rows {@link RowKind#INSERT}.</li>
 *   <li><b>DELETE split with DV/positional deletes</b>: read the data file, KEEP only
 *       the rows the DV marks deleted (those are the rows being retracted). Tag
 *       {@link RowKind#DELETE}.</li>
 *   <li><b>DELETE split with equality deletes</b>: read the data file, project
 *       equality-key columns, and KEEP rows whose equality-key tuple is in the
 *       loaded equality-set. Tag {@link RowKind#DELETE}.</li>
 * </ul>
 */
public final class IcebergCdcSplitReader implements SplitReader<TimestampedRow, IcebergCdcSplit> {

    private static final Logger LOG = LoggerFactory.getLogger(IcebergCdcSplitReader.class);
    private static final int MAX_EQUALITY_DELETE_CACHE_ENTRIES = 128;

    private final IcebergCdcEnumerator.TableSupplier tableSupplier;
    /** SQL-declared schema (possibly projected). Drives what the reader emits. */
    private final Schema requestedSchema;
    private final Counter rowsEmitted;
    private final Counter splitsOpened;
    private final Map<String, CachedEqualityDeleteSet> equalityDeleteCache =
            new LinkedHashMap<String, CachedEqualityDeleteSet>(16, 0.75f, true) {
                @Override
                protected boolean removeEldestEntry(Map.Entry<String, CachedEqualityDeleteSet> eldest) {
                    return size() > MAX_EQUALITY_DELETE_CACHE_ENTRIES;
                }
            };

    private final Deque<IcebergCdcSplit> queue = new ArrayDeque<>();
    private IcebergCdcSplit current;
    private CloseableIterable<RowData> currentIter;
    private Iterator<RowData> currentRowIter;
    private RowKind currentKind;
    private long currentPos;
    private PositionDeleteIndex currentDeletes;
    /** True when attached delete files select rows to emit; false means suppress matches. */
    private boolean currentDeletesSelectRows;

    /** Equality-key filters, populated for equality-delete splits; empty otherwise. */
    private List<EqualityFilter> currentEqualities = List.of();
    /**
     * For equality-delete splits where SQL projection has dropped some equality-key columns,
     * we read those extra columns from the data file and then project the row down to the
     * user-requested schema before emitting. {@code projectedFieldIndices} is the mapping:
     * for each output position, the index of that column within the row we read.
     */
    private RowDataProjection currentProjection;

    public IcebergCdcSplitReader(IcebergCdcEnumerator.TableSupplier tableSupplier,
                                 Schema requestedSchema) {
        this(tableSupplier, requestedSchema, null);
    }

    public IcebergCdcSplitReader(IcebergCdcEnumerator.TableSupplier tableSupplier,
                                 Schema requestedSchema,
                                 SourceReaderContext context) {
        this.tableSupplier = tableSupplier;
        this.requestedSchema = requestedSchema;
        MetricGroup metrics = context == null ? null : context.metricGroup();
        this.rowsEmitted = metrics == null ? null : metrics.counter("rowsEmitted");
        this.splitsOpened = metrics == null ? null : metrics.counter("splitsOpened");
    }

    @Override
    public RecordsWithSplitIds<TimestampedRow> fetch() throws IOException {
        if (current == null) {
            current = queue.pollFirst();
            if (current == null) return new RecordsBySplits.Builder<TimestampedRow>().build();
            openCurrent();
        }

        RecordsBySplits.Builder<TimestampedRow> builder = new RecordsBySplits.Builder<>();
        long ts = current.commitTimestampMillis();
        int batch = 0;
        while (currentRowIter != null && currentRowIter.hasNext() && batch < 1024) {
            RowData row = currentRowIter.next();
            long pos = currentPos++;

            if (!keepRow(row, pos)) continue;

            // If we read extra columns to support equality-delete keying that the SQL
            // projection didn't ask for, narrow the row back down to the user schema.
            RowData out = currentProjection == null
                    ? row
                    : currentProjection.project(row, pos, current);
            out.setRowKind(currentKind);
            builder.add(current.splitId(), new TimestampedRow(out, ts, currentPos));
            if (rowsEmitted != null) rowsEmitted.inc();
            batch++;
        }

        if (currentRowIter == null || !currentRowIter.hasNext()) {
            try { if (currentIter != null) currentIter.close(); } catch (Exception ignored) {}
            builder.addFinishedSplit(current.splitId());
            current = null;
            currentIter = null;
            currentRowIter = null;
            currentDeletes = null;
            currentEqualities = List.of();
            currentProjection = null;
            currentPos = 0;
        }
        return builder.build();
    }

    private boolean keepRow(RowData row, long pos) {
        boolean hasDeleteFilter = currentDeletes != null || !currentEqualities.isEmpty();
        if (!hasDeleteFilter) return true;

        boolean matchesDelete = currentDeletes != null && currentDeletes.isDeleted(pos);
        if (!matchesDelete) {
            for (EqualityFilter equality : currentEqualities) {
                if (equality.matches(row)) {
                    matchesDelete = true;
                    break;
                }
            }
        }
        return currentDeletesSelectRows ? matchesDelete : !matchesDelete;
    }

    private void openCurrent() throws IOException {
        if (splitsOpened != null) splitsOpened.inc();
        Table table = tableSupplier.get();
        table.refresh();
        FileIO io = table.io();

        currentKind = mapKind(current.operation());

        // Classify deletes on the split by content type.
        Map<String, List<IcebergCdcSplit.DeleteFileRef>> equalityByIds = new LinkedHashMap<>();
        Map<String, int[]> equalityIdsByKey = new LinkedHashMap<>();
        List<IcebergCdcSplit.DeleteFileRef> dvPos = new ArrayList<>();
        for (IcebergCdcSplit.DeleteFileRef ref : current.deletes()) {
            if ("EQUALITY_DELETES".equals(ref.content)) {
                String key = Arrays.toString(ref.equalityFieldIds);
                equalityByIds.computeIfAbsent(key, ignored -> new ArrayList<>()).add(ref);
                equalityIdsByKey.putIfAbsent(key, ref.equalityFieldIds);
            } else {
                dvPos.add(ref);
            }
        }

        currentDeletesSelectRows = current.deletesSelectRows();

        Schema baseRequested = requestedSchema;
        Schema readSchema = userWantsMetadataCols(baseRequested)
                ? stripMetadataCols(baseRequested)
                : baseRequested;

        // Resolve the partition spec for this split's data file, so reconstructed delete
        // files carry the right spec/partition metadata (positional/DV deletes for
        // partitioned tables would otherwise be silently misrouted).
        org.apache.iceberg.PartitionSpec spec = table.specs().get(current.partitionSpecId());
        if (spec == null) spec = table.spec();

        for (int[] equalityIds : equalityIdsByKey.values()) {
            readSchema = augmentWithEqualityKeys(readSchema, equalityIds, table.schema());
        }

        if (!equalityByIds.isEmpty()) {
            List<EqualityFilter> filters = new ArrayList<>(equalityByIds.size());
            for (Map.Entry<String, List<IcebergCdcSplit.DeleteFileRef>> e : equalityByIds.entrySet()) {
                int[] equalityIds = equalityIdsByKey.get(e.getKey());
                filters.add(buildEqualityFilter(
                        io, e.getValue(), equalityIds, table.schema(), readSchema,
                        equalityDeleteCache));
            }
            currentEqualities = filters;
        } else {
            currentEqualities = List.of();
        }

        currentProjection = readSchema.sameSchema(baseRequested)
                ? null
                : RowDataProjection.from(readSchema, baseRequested);

        if (!dvPos.isEmpty()) {
            List<DeleteFile> deleteFiles = new ArrayList<>();
            for (IcebergCdcSplit.DeleteFileRef ref : dvPos) {
                deleteFiles.add(reconstructPositionalDeleteFile(ref, current.dataFilePath(), spec));
            }
            BaseDeleteLoader loader = new BaseDeleteLoader(
                    df -> io.newInputFile(df.path().toString()));
            currentDeletes = loader.loadPositionDeletes(deleteFiles, current.dataFilePath());
        } else {
            currentDeletes = null;
        }

        final Schema finalReadSchema = readSchema;
        InputFile in = io.newInputFile(current.dataFilePath(), current.fileSizeBytes());
        currentIter = Parquet.read(in)
                .project(finalReadSchema)
                .createReaderFunc(t -> FlinkParquetReaders.buildReader(finalReadSchema, t))
                .build();
        currentRowIter = currentIter.iterator();
        currentPos = 0L;
        skipToStartPosition(current.startOffset());
    }

    private void skipToStartPosition(long startPosition) {
        if (startPosition <= 0) return;
        while (currentRowIter != null && currentRowIter.hasNext() && currentPos < startPosition) {
            currentRowIter.next();
            currentPos++;
        }
        if (currentPos < startPosition) {
            LOG.warn("Split {} requested resume at row {}, but data file ended at row {}",
                    current.splitId(), startPosition, currentPos);
        }
    }

    private static EqualityFilter buildEqualityFilter(FileIO io,
                                                     List<IcebergCdcSplit.DeleteFileRef> refs,
                                                     int[] equalityIds,
                                                     Schema tableSchema,
                                                     Schema readSchema,
                                                     Map<String, CachedEqualityDeleteSet> cache) {
        // The equality-key column types come from the table schema (authoritative).
        // Their indices in the row we'll read come from readSchema.
        List<Types.NestedField> deleteFields = new ArrayList<>(equalityIds.length);
        int[] indicesInRead = new int[equalityIds.length];
        for (int i = 0; i < equalityIds.length; i++) {
            int fieldId = equalityIds[i];
            Types.NestedField fromTable = tableSchema.findField(fieldId);
            if (fromTable == null) {
                throw new IllegalStateException("Equality field id " + fieldId
                        + " not in table schema");
            }
            deleteFields.add(fromTable);
            int idx = -1;
            for (int j = 0; j < readSchema.columns().size(); j++) {
                if (readSchema.columns().get(j).fieldId() == fieldId) { idx = j; break; }
            }
            if (idx < 0) {
                throw new IllegalStateException("Equality field id " + fieldId
                        + " not in read schema; augmentWithEqualityKeys must be called first");
            }
            indicesInRead[i] = idx;
        }
        Schema deleteSchema = new Schema(deleteFields);

        String cacheKey = equalityCacheKey(refs, equalityIds);
        CachedEqualityDeleteSet cached = cache.get(cacheKey);
        if (cached == null) {
            List<DeleteFile> deleteFiles = new ArrayList<>(refs.size());
            for (IcebergCdcSplit.DeleteFileRef ref : refs) {
                deleteFiles.add(reconstructEqualityDeleteFile(ref, equalityIds));
            }
            BaseDeleteLoader loader = new BaseDeleteLoader(
                    df -> io.newInputFile(df.path().toString()));
            cached = new CachedEqualityDeleteSet(
                    loader.loadEqualityDeletes(deleteFiles, deleteSchema), deleteSchema);
            cache.put(cacheKey, cached);
        }

        return new EqualityFilter(cached.set(), cached.schema(), indicesInRead);
    }

    private static String equalityCacheKey(List<IcebergCdcSplit.DeleteFileRef> refs,
                                           int[] equalityIds) {
        StringBuilder key = new StringBuilder(Arrays.toString(equalityIds));
        for (IcebergCdcSplit.DeleteFileRef ref : refs) {
            key.append('|')
                    .append(ref.path)
                    .append('@').append(ref.fileSizeBytes)
                    .append(':').append(ref.sequenceNumber)
                    .append(':').append(ref.contentOffset)
                    .append(':').append(ref.contentSizeInBytes);
        }
        return key.toString();
    }

    /** If any equality field is missing from {@code readSchema}, splice it in. */
    private static Schema augmentWithEqualityKeys(Schema readSchema, int[] equalityIds, Schema tableSchema) {
        java.util.Set<Integer> have = new java.util.HashSet<>();
        for (Types.NestedField f : readSchema.columns()) have.add(f.fieldId());
        List<Types.NestedField> all = new ArrayList<>(readSchema.columns());
        for (int id : equalityIds) {
            if (have.add(id)) {
                Types.NestedField src = tableSchema.findField(id);
                if (src == null) {
                    throw new IllegalStateException("Equality field id " + id + " not in table schema");
                }
                all.add(src);
            }
        }
        return new Schema(all);
    }

    /**
     * Projects a wider {@link RowData} down to a narrower one by typed copying of each
     * requested column. We materialize a {@link org.apache.flink.table.data.GenericRowData}
     * per row because the projected row is handed off to {@link RecordsBySplits.Builder},
     * which stores references and may serialize across operators.
     */
    private static final class RowDataProjection implements java.io.Serializable {
        private static final long serialVersionUID = 1L;
        private final int[] sourceIndices;
        private final org.apache.flink.table.types.logical.LogicalType[] columnTypes;
        private static final int SOURCE_ROW_ID = -1;
        private static final int SOURCE_LAST_UPDATED_SEQUENCE_NUMBER = -2;
        private static final int SOURCE_COMMIT_SNAPSHOT_ID = -3;
        private static final int SOURCE_CHANGE_ORDINAL = -4;
        private static final int SOURCE_CHANGE_TYPE = -5;
        private static final int SOURCE_FILE_PATH = -6;
        private static final int SOURCE_ROW_POSITION = -7;
        private static final int SOURCE_SPEC_ID = -8;
        private static final int SOURCE_COMMIT_TIMESTAMP = -9;

        private RowDataProjection(int[] sourceIndices,
                                  org.apache.flink.table.types.logical.LogicalType[] types) {
            this.sourceIndices = sourceIndices;
            this.columnTypes = types;
        }

        static RowDataProjection from(Schema readSchema, Schema requestedSchema) {
            int[] map = new int[requestedSchema.columns().size()];
            org.apache.flink.table.types.logical.LogicalType[] types =
                    new org.apache.flink.table.types.logical.LogicalType[requestedSchema.columns().size()];
            for (int i = 0; i < requestedSchema.columns().size(); i++) {
                int fid = requestedSchema.columns().get(i).fieldId();
                if (fid == MetadataColumns.ROW_ID.fieldId()) {
                    map[i] = SOURCE_ROW_ID;
                    types[i] = org.apache.iceberg.flink.FlinkSchemaUtil.convert(
                            requestedSchema.columns().get(i).type());
                    continue;
                }
                if (fid == MetadataColumns.LAST_UPDATED_SEQUENCE_NUMBER.fieldId()) {
                    map[i] = SOURCE_LAST_UPDATED_SEQUENCE_NUMBER;
                    types[i] = org.apache.iceberg.flink.FlinkSchemaUtil.convert(
                            requestedSchema.columns().get(i).type());
                    continue;
                }
                if (fid == MetadataColumns.COMMIT_SNAPSHOT_ID.fieldId()) {
                    map[i] = SOURCE_COMMIT_SNAPSHOT_ID;
                    types[i] = org.apache.iceberg.flink.FlinkSchemaUtil.convert(
                            requestedSchema.columns().get(i).type());
                    continue;
                }
                if (fid == IcebergCdcMetadata.COMMIT_TIMESTAMP_FIELD_ID) {
                    map[i] = SOURCE_COMMIT_TIMESTAMP;
                    types[i] = org.apache.iceberg.flink.FlinkSchemaUtil.convert(
                            requestedSchema.columns().get(i).type());
                    continue;
                }
                if (fid == MetadataColumns.CHANGE_ORDINAL.fieldId()) {
                    map[i] = SOURCE_CHANGE_ORDINAL;
                    types[i] = org.apache.iceberg.flink.FlinkSchemaUtil.convert(
                            requestedSchema.columns().get(i).type());
                    continue;
                }
                if (fid == MetadataColumns.CHANGE_TYPE.fieldId()) {
                    map[i] = SOURCE_CHANGE_TYPE;
                    types[i] = org.apache.iceberg.flink.FlinkSchemaUtil.convert(
                            requestedSchema.columns().get(i).type());
                    continue;
                }
                if (fid == MetadataColumns.FILE_PATH.fieldId()) {
                    map[i] = SOURCE_FILE_PATH;
                    types[i] = org.apache.iceberg.flink.FlinkSchemaUtil.convert(
                            requestedSchema.columns().get(i).type());
                    continue;
                }
                if (fid == MetadataColumns.ROW_POSITION.fieldId()) {
                    map[i] = SOURCE_ROW_POSITION;
                    types[i] = org.apache.iceberg.flink.FlinkSchemaUtil.convert(
                            requestedSchema.columns().get(i).type());
                    continue;
                }
                if (fid == MetadataColumns.SPEC_ID.fieldId()) {
                    map[i] = SOURCE_SPEC_ID;
                    types[i] = org.apache.iceberg.flink.FlinkSchemaUtil.convert(
                            requestedSchema.columns().get(i).type());
                    continue;
                }
                int found = -1;
                for (int j = 0; j < readSchema.columns().size(); j++) {
                    if (readSchema.columns().get(j).fieldId() == fid) { found = j; break; }
                }
                if (found < 0) {
                    throw new IllegalStateException("Requested field id " + fid + " not in read schema");
                }
                map[i] = found;
                types[i] = org.apache.iceberg.flink.FlinkSchemaUtil.convert(
                        requestedSchema.columns().get(i).type());
            }
            return new RowDataProjection(map, types);
        }

        RowData project(RowData backing, long rowPosition, IcebergCdcSplit split) {
            org.apache.flink.table.data.GenericRowData out =
                    new org.apache.flink.table.data.GenericRowData(backing.getRowKind(), sourceIndices.length);
            for (int i = 0; i < sourceIndices.length; i++) {
                int src = sourceIndices[i];
                if (src == SOURCE_ROW_ID) {
                    out.setField(i, split.firstRowId() < 0 ? null : split.firstRowId() + rowPosition);
                    continue;
                }
                if (src == SOURCE_LAST_UPDATED_SEQUENCE_NUMBER) {
                    out.setField(i, split.dataSequenceNumber() < 0 ? null : split.dataSequenceNumber());
                    continue;
                }
                if (src == SOURCE_COMMIT_SNAPSHOT_ID) {
                    out.setField(i, split.commitSnapshotId());
                    continue;
                }
                if (src == SOURCE_COMMIT_TIMESTAMP) {
                    out.setField(i, org.apache.flink.table.data.TimestampData.fromEpochMillis(
                            split.commitTimestampMillis()));
                    continue;
                }
                if (src == SOURCE_CHANGE_ORDINAL) {
                    out.setField(i, split.changeOrdinal());
                    continue;
                }
                if (src == SOURCE_CHANGE_TYPE) {
                    out.setField(i, org.apache.flink.table.data.StringData.fromString(
                            split.operation().name()));
                    continue;
                }
                if (src == SOURCE_FILE_PATH) {
                    out.setField(i, org.apache.flink.table.data.StringData.fromString(
                            split.dataFilePath()));
                    continue;
                }
                if (src == SOURCE_ROW_POSITION) {
                    out.setField(i, rowPosition);
                    continue;
                }
                if (src == SOURCE_SPEC_ID) {
                    out.setField(i, split.partitionSpecId());
                    continue;
                }
                if (backing.isNullAt(src)) { out.setField(i, null); continue; }
                out.setField(i, readByType(backing, src, columnTypes[i]));
            }
            return out;
        }

        private static Object readByType(RowData row, int idx,
                                         org.apache.flink.table.types.logical.LogicalType type) {
            switch (type.getTypeRoot()) {
                case BOOLEAN: return row.getBoolean(idx);
                case TINYINT: return row.getByte(idx);
                case SMALLINT: return row.getShort(idx);
                case INTEGER:
                case DATE: return row.getInt(idx);
                case TIME_WITHOUT_TIME_ZONE: return row.getInt(idx);
                case BIGINT: return row.getLong(idx);
                case FLOAT: return row.getFloat(idx);
                case DOUBLE: return row.getDouble(idx);
                case CHAR:
                case VARCHAR: return row.getString(idx);
                case BINARY:
                case VARBINARY: return row.getBinary(idx);
                case DECIMAL: {
                    org.apache.flink.table.types.logical.DecimalType d =
                            (org.apache.flink.table.types.logical.DecimalType) type;
                    return row.getDecimal(idx, d.getPrecision(), d.getScale());
                }
                case TIMESTAMP_WITHOUT_TIME_ZONE: {
                    org.apache.flink.table.types.logical.TimestampType tt =
                            (org.apache.flink.table.types.logical.TimestampType) type;
                    return row.getTimestamp(idx, tt.getPrecision());
                }
                case TIMESTAMP_WITH_LOCAL_TIME_ZONE: {
                    org.apache.flink.table.types.logical.LocalZonedTimestampType ltt =
                            (org.apache.flink.table.types.logical.LocalZonedTimestampType) type;
                    return row.getTimestamp(idx, ltt.getPrecision());
                }
                case ARRAY: return row.getArray(idx);
                case MAP:
                case MULTISET: return row.getMap(idx);
                case ROW: {
                    org.apache.flink.table.types.logical.RowType rowType =
                            (org.apache.flink.table.types.logical.RowType) type;
                    return row.getRow(idx, rowType.getFieldCount());
                }
                default:
                    throw new UnsupportedOperationException(
                            "Projection unsupported for type " + type);
            }
        }
    }

    private static DeleteFile reconstructPositionalDeleteFile(
            IcebergCdcSplit.DeleteFileRef ref, String referencedDataFilePath, PartitionSpec spec) {
        FileMetadata.Builder b = FileMetadata.deleteFileBuilder(spec)
                .ofPositionDeletes()
                .withPath(ref.path)
                .withFileSizeInBytes(ref.fileSizeBytes)
                .withRecordCount(1)
                .withReferencedDataFile(referencedDataFilePath);
        if (ref.contentOffset >= 0) b.withContentOffset(ref.contentOffset);
        if (ref.contentSizeInBytes >= 0) b.withContentSizeInBytes(ref.contentSizeInBytes);
        return b.build();
    }

    private static DeleteFile reconstructEqualityDeleteFile(
            IcebergCdcSplit.DeleteFileRef ref, int[] equalityFieldIds) {
        return FileMetadata.deleteFileBuilder(PartitionSpec.unpartitioned())
                .ofEqualityDeletes(equalityFieldIds)
                .withPath(ref.path)
                .withFileSizeInBytes(ref.fileSizeBytes)
                .withRecordCount(1)
                .build();
    }

    private static boolean userWantsMetadataCols(Schema schema) {
        for (Types.NestedField f : schema.columns()) {
            if (isReadableMetadataColumn(f.fieldId())) return true;
        }
        return false;
    }

    private static Schema stripMetadataCols(Schema schema) {
        java.util.List<Types.NestedField> filtered = new java.util.ArrayList<>();
        for (Types.NestedField f : schema.columns()) {
            if (isReadableMetadataColumn(f.fieldId())) continue;
            filtered.add(f);
        }
        return new Schema(filtered);
    }

    private static boolean isReadableMetadataColumn(int fieldId) {
        return fieldId == MetadataColumns.ROW_ID.fieldId()
                || fieldId == MetadataColumns.LAST_UPDATED_SEQUENCE_NUMBER.fieldId()
                || fieldId == MetadataColumns.COMMIT_SNAPSHOT_ID.fieldId()
                || fieldId == IcebergCdcMetadata.COMMIT_TIMESTAMP_FIELD_ID
                || fieldId == MetadataColumns.CHANGE_ORDINAL.fieldId()
                || fieldId == MetadataColumns.CHANGE_TYPE.fieldId()
                || fieldId == MetadataColumns.FILE_PATH.fieldId()
                || fieldId == MetadataColumns.ROW_POSITION.fieldId()
                || fieldId == MetadataColumns.SPEC_ID.fieldId();
    }

    private static RowKind mapKind(ChangelogOperation op) {
        switch (op) {
            case INSERT: return RowKind.INSERT;
            case DELETE: return RowKind.DELETE;
            case UPDATE_BEFORE: return RowKind.UPDATE_BEFORE;
            case UPDATE_AFTER: return RowKind.UPDATE_AFTER;
            default: throw new IllegalStateException("Unknown ChangelogOperation: " + op);
        }
    }

    @Override
    public void handleSplitsChanges(SplitsChange<IcebergCdcSplit> change) {
        if (change instanceof SplitsAddition) queue.addAll(change.splits());
    }

    @Override public void wakeUp() {}

    @Override public void close() {
        if (currentIter != null) {
            try { currentIter.close(); } catch (Exception ignored) {}
        }
    }

    private record CachedEqualityDeleteSet(StructLikeSet set, Schema schema) {}

    /** Tests an in-memory equality-key set against the projected columns of a {@link RowData}. */
    private static final class EqualityFilter {
        private final StructLikeSet set;
        private final Schema deleteSchema;
        private final int[] projectedIndices;

        EqualityFilter(StructLikeSet set, Schema deleteSchema, int[] projectedIndices) {
            this.set = set;
            this.deleteSchema = deleteSchema;
            this.projectedIndices = projectedIndices;
        }

        boolean matches(RowData row) {
            // Build a Record-like StructLike from the projected RowData columns and check membership.
            org.apache.iceberg.data.GenericRecord rec =
                    org.apache.iceberg.data.GenericRecord.create(deleteSchema);
            for (int i = 0; i < projectedIndices.length; i++) {
                int idx = projectedIndices[i];
                Types.NestedField f = deleteSchema.columns().get(i);
                rec.set(i, readField(row, idx, f.type()));
            }
            return set.contains(rec);
        }

        private static Object readField(RowData row, int idx, org.apache.iceberg.types.Type type) {
            if (row.isNullAt(idx)) return null;
            switch (type.typeId()) {
                case STRING: return row.getString(idx).toString();
                case BOOLEAN: return row.getBoolean(idx);
                case INTEGER: return row.getInt(idx);
                case LONG: return row.getLong(idx);
                case FLOAT: return row.getFloat(idx);
                case DOUBLE: return row.getDouble(idx);
                case DATE: {
                    // Iceberg DATE compares as LocalDate; row stores days-since-epoch as int.
                    return java.time.LocalDate.ofEpochDay(row.getInt(idx));
                }
                case TIME: {
                    // Iceberg TIME compares as LocalTime; row stores micros-of-day as int.
                    return java.time.LocalTime.ofNanoOfDay(((long) row.getInt(idx)) * 1000L);
                }
                case TIMESTAMP: {
                    org.apache.iceberg.types.Types.TimestampType tt =
                            (org.apache.iceberg.types.Types.TimestampType) type;
                    org.apache.flink.table.data.TimestampData td = row.getTimestamp(idx, 6);
                    if (tt.shouldAdjustToUTC()) {
                        return java.time.OffsetDateTime.ofInstant(
                                td.toInstant(), java.time.ZoneOffset.UTC);
                    }
                    return td.toLocalDateTime();
                }
                case DECIMAL: {
                    org.apache.iceberg.types.Types.DecimalType dt =
                            (org.apache.iceberg.types.Types.DecimalType) type;
                    return row.getDecimal(idx, dt.precision(), dt.scale()).toBigDecimal();
                }
                case BINARY: return java.nio.ByteBuffer.wrap(row.getBinary(idx));
                case FIXED: return java.nio.ByteBuffer.wrap(row.getBinary(idx));
                case UUID: {
                    // Iceberg UUID stored as 16-byte fixed; row reads as binary.
                    byte[] b = row.getBinary(idx);
                    java.nio.ByteBuffer bb = java.nio.ByteBuffer.wrap(b);
                    long high = bb.getLong();
                    long low = bb.getLong();
                    return new java.util.UUID(high, low);
                }
                default:
                    throw new UnsupportedOperationException(
                            "Equality delete on type " + type + " not supported");
            }
        }
    }
}
