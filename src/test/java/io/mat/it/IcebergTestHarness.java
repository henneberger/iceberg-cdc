package io.mat.it;

import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.AppendFiles;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.data.GenericAppenderFactory;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.encryption.EncryptionUtil;
import org.apache.iceberg.hadoop.HadoopCatalog;
import org.apache.iceberg.io.DataWriter;
import org.apache.iceberg.io.OutputFile;

import java.util.List;
import java.util.UUID;

/** Test helper: creates Iceberg tables in a Hadoop catalog and appends data files directly. */
public final class IcebergTestHarness implements AutoCloseable {

    private final HadoopCatalog catalog;
    private final String warehouse;

    public IcebergTestHarness(String warehouse) {
        this.warehouse = warehouse;
        this.catalog = new HadoopCatalog();
        java.util.Map<String, String> props = new java.util.HashMap<>();
        props.put("warehouse", warehouse);
        catalog.setConf(new Configuration());
        catalog.initialize("test", props);
    }

    public Table createTable(TableIdentifier id, Schema schema) {
        return createTable(id, schema, PartitionSpec.unpartitioned(), "2");
    }

    /** Default v3 for sink tables that need row-lineage CDC tracking. */
    public Table createV3Table(TableIdentifier id, Schema schema) {
        return createTable(id, schema, PartitionSpec.unpartitioned(), "3");
    }

    public Table createTable(TableIdentifier id, Schema schema, PartitionSpec spec) {
        return createTable(id, schema, spec, "2");
    }

    public Table createTable(TableIdentifier id, Schema schema, PartitionSpec spec, String formatVersion) {
        if (catalog.tableExists(id)) catalog.dropTable(id);
        return catalog.createTable(id, schema, spec,
                java.util.Map.of("format-version", formatVersion));
    }

    /** Append a list of records as one new data file in a fresh snapshot. */
    public void appendRecords(Table table, List<Record> records) throws Exception {
        GenericAppenderFactory af = new GenericAppenderFactory(table.schema(), table.spec());
        String filename = "data-" + UUID.randomUUID() + ".parquet";
        OutputFile out = table.io().newOutputFile(table.location() + "/data/" + filename);

        DataWriter<Record> writer = af.newDataWriter(
                EncryptionUtil.plainAsEncryptedOutput(out),
                FileFormat.PARQUET,
                null);
        try (writer) {
            for (Record r : records) writer.write(r);
        }
        DataFile dataFile = writer.toDataFile();

        AppendFiles append = table.newAppend();
        append.appendFile(dataFile);
        append.commit();
        table.refresh();
    }

    public HadoopCatalog catalog() { return catalog; }
    public String warehouse() { return warehouse; }

    @Override
    public void close() throws Exception {
        catalog.close();
    }
}
