package io.mat.sql;

import io.mat.source.SplitAssignmentMode;
import io.mat.source.StartupMode;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.factories.DynamicTableSourceFactory;
import org.apache.flink.table.factories.FactoryUtil;
import org.apache.flink.table.types.logical.RowType;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.CatalogUtil;
import org.apache.iceberg.catalog.Catalog;

import java.io.Serializable;
import java.time.Duration;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * SQL factory for the {@code iceberg-cdc} CDC source connector.
 *
 * <p>SQL DDL options:
 * <ul>
 *   <li>{@code catalog-impl} (required) — fully-qualified Iceberg catalog class.</li>
 *   <li>{@code warehouse} (required) — catalog warehouse URI.</li>
 *   <li>{@code table} (required) — fully-qualified table identifier (e.g. {@code db.t}).</li>
 *   <li>{@code catalog.uri} — for REST/JDBC catalogs.</li>
 *   <li>{@code catalog.region} — for AWS catalogs (default {@code us-east-1}).</li>
 *   <li>{@code discovery-interval} — snapshot poll cadence (default 30s).</li>
 *   <li>{@code allowed-lateness} — bounded out-of-orderness for the watermark (default 30s).</li>
 *   <li>{@code bounded} — set true to read existing snapshots and stop (default false).</li>
 * </ul>
 */
public final class IcebergCdcDynamicTableSourceFactory implements DynamicTableSourceFactory {

    public static final String IDENTIFIER = "iceberg-cdc";

    public static final ConfigOption<String> CATALOG_IMPL = ConfigOptions
            .key("catalog-impl").stringType().noDefaultValue();
    public static final ConfigOption<String> CATALOG_URI = ConfigOptions
            .key("catalog.uri").stringType().noDefaultValue();
    public static final ConfigOption<String> WAREHOUSE = ConfigOptions
            .key("warehouse").stringType().noDefaultValue();
    public static final ConfigOption<String> CATALOG_REGION = ConfigOptions
            .key("catalog.region").stringType().defaultValue("us-east-1");
    public static final ConfigOption<String> TABLE = ConfigOptions
            .key("table").stringType().noDefaultValue();
    public static final ConfigOption<Duration> DISCOVERY_INTERVAL = ConfigOptions
            .key("discovery-interval").durationType().defaultValue(Duration.ofSeconds(30));
    public static final ConfigOption<Duration> ALLOWED_LATENESS = ConfigOptions
            .key("allowed-lateness").durationType().defaultValue(Duration.ofSeconds(30));
    public static final ConfigOption<Boolean> BOUNDED = ConfigOptions
            .key("bounded").booleanType().defaultValue(false);
    public static final ConfigOption<String> SPLIT_ASSIGNMENT_MODE = ConfigOptions
            .key("split-assignment-mode").stringType().defaultValue("ordered");
    public static final ConfigOption<String> SCAN_STARTUP_MODE = ConfigOptions
            .key("scan.startup.mode").stringType().defaultValue("earliest");

    @Override public String factoryIdentifier() { return IDENTIFIER; }

    @Override public Set<ConfigOption<?>> requiredOptions() {
        Set<ConfigOption<?>> s = new HashSet<>();
        s.add(CATALOG_IMPL); s.add(WAREHOUSE); s.add(TABLE);
        return s;
    }

    @Override public Set<ConfigOption<?>> optionalOptions() {
        Set<ConfigOption<?>> s = new HashSet<>();
        s.add(CATALOG_URI); s.add(CATALOG_REGION);
        s.add(DISCOVERY_INTERVAL); s.add(ALLOWED_LATENESS); s.add(BOUNDED);
        s.add(SPLIT_ASSIGNMENT_MODE); s.add(SCAN_STARTUP_MODE);
        return s;
    }

    @Override
    public DynamicTableSource createDynamicTableSource(Context context) {
        FactoryUtil.TableFactoryHelper helper = FactoryUtil.createTableFactoryHelper(this, context);
        // Don't fail on unknown 'catalog.*' options — they pass through.
        helper.validateExcept("catalog.");
        ReadableConfig cfg = helper.getOptions();

        Options o = new Options();
        o.catalogImpl = cfg.get(CATALOG_IMPL);
        o.catalogUri = cfg.get(CATALOG_URI);
        o.warehouse = cfg.get(WAREHOUSE);
        o.region = cfg.get(CATALOG_REGION);
        o.tableId = cfg.get(TABLE);
        o.discoveryInterval = cfg.get(DISCOVERY_INTERVAL);
        o.allowedLateness = cfg.get(ALLOWED_LATENESS);
        o.bounded = cfg.get(BOUNDED);
        try {
            o.splitAssignmentMode = SplitAssignmentMode.fromOption(cfg.get(SPLIT_ASSIGNMENT_MODE));
            o.startupMode = StartupMode.fromOption(cfg.get(SCAN_STARTUP_MODE));
        } catch (IllegalArgumentException e) {
            throw new ValidationException(e.getMessage(), e);
        }

        if (o.discoveryInterval == null
                || o.discoveryInterval.isZero()
                || o.discoveryInterval.isNegative()) {
            throw new ValidationException("'discovery-interval' must be greater than 0");
        }
        if (o.allowedLateness == null || o.allowedLateness.isNegative()) {
            throw new ValidationException("'allowed-lateness' must be >= 0");
        }

        // Capture every 'catalog.<x>' option from the DDL options map. These are passed
        // verbatim to the Iceberg catalog so REST/JDBC/Glue/Nessie etc. work.
        Map<String, String> raw = context.getCatalogTable().getOptions();
        for (Map.Entry<String, String> e : raw.entrySet()) {
            if (e.getKey().startsWith("catalog.")) {
                String stripped = e.getKey().substring("catalog.".length());
                if (stripped.isEmpty() || stripped.equals("uri") || stripped.equals("region")) continue;
                o.catalogProps.put(stripped, e.getValue());
            }
        }

        ResolvedSchema schema = context.getCatalogTable().getResolvedSchema();
        RowType rowType = (RowType) schema.toPhysicalRowDataType().getLogicalType();

        return new IcebergCdcDynamicTableSource(rowType, o, makeCatalogFactory(o));
    }

    static CatalogFactory makeCatalogFactory(Options o) {
        return () -> {
            Map<String, String> props = new HashMap<>();
            props.put(CatalogProperties.CATALOG_IMPL, o.catalogImpl);
            props.put(CatalogProperties.WAREHOUSE_LOCATION, o.warehouse);
            if (o.catalogUri != null) props.put(CatalogProperties.URI, o.catalogUri);
            if (o.region != null) props.put("client.region", o.region);
            // Pass-through user-supplied catalog.* properties (auth, signing, etc).
            props.putAll(o.catalogProps);
            org.apache.hadoop.conf.Configuration hadoopConf = new org.apache.hadoop.conf.Configuration();
            return CatalogUtil.buildIcebergCatalog("mat", props, hadoopConf);
        };
    }

    public static final class Options implements Serializable {
        private static final long serialVersionUID = 1L;
        public String catalogImpl;
        public String catalogUri;
        public String warehouse;
        public String region;
        public String tableId;
        public Duration discoveryInterval;
        public Duration allowedLateness;
        public boolean bounded;
        public SplitAssignmentMode splitAssignmentMode;
        public StartupMode startupMode;
        /** Arbitrary {@code catalog.<key>=value} props from the SQL WITH clause. */
        public Map<String, String> catalogProps = new HashMap<>();
    }

    @FunctionalInterface
    public interface CatalogFactory extends Serializable { Catalog create(); }
}
