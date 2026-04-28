package dev.henneberger.it;

import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Smoke-tests Iceberg v3 against the test harness — confirms the catalog
 * accepts {@code format-version=3} and reports it back. The actual pipeline
 * uses v2 today because Iceberg 1.7.1's writer side for v3 row-lineage as
 * physical metadata columns is still firming up — the sink stamps lineage
 * as user fields instead. This test pins the behavior so that when v3
 * writer support stabilizes, we can flip the harness to {@code createV3Table}
 * and the sink to write the lineage columns through the v3 metadata path
 * without any other code change.
 */
class IcebergV3SmokeTest {

    @TempDir
    Path tempDir;

    @Test
    void v3TableCreationAndFormatVersionPersistence() throws Exception {
        String warehouse = tempDir.toUri().toString();
        TableIdentifier id = TableIdentifier.of("v3", "demo");

        Schema schema = new Schema(
                Types.NestedField.required(1, "k", Types.StringType.get()),
                Types.NestedField.required(2, "v", Types.LongType.get()));

        try (IcebergTestHarness harness = new IcebergTestHarness(warehouse)) {
            Table tbl = harness.createV3Table(id, schema);

            // Iceberg's TableMetadata.formatVersion() exposes this; via Table API:
            int formatVersion = ((org.apache.iceberg.BaseTable) tbl).operations().current().formatVersion();
            assertEquals(3, formatVersion, "table must report format-version=3");

            // Reload via catalog to confirm it persists across handles.
            Table reloaded = harness.catalog().loadTable(id);
            int reloadedVersion = ((org.apache.iceberg.BaseTable) reloaded).operations().current().formatVersion();
            assertEquals(3, reloadedVersion, "reloaded table must still report v3");

            // Property surface for downstream readers.
            assertTrue(
                    "3".equals(reloaded.properties().get("format-version"))
                    || tbl.properties().get("format-version") == null,
                    "table either advertises format-version=3 or doesn't surface it as a property");
        }
    }
}
