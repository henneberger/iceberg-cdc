-- Unbounded temporal join SELECT used by SqlTemporalJoinCdcE2EIT.
--
-- Start this query before the code generator commits source snapshots.
-- Both sides use row payload event time:
--   orders.order_time for facts
--   customer_events.change_time for dimension versions

SET 'pipeline.name' = 'mat-e2e-codegen-cdc-temporal-select';
SET 'execution.runtime-mode' = 'streaming';

CREATE TEMPORARY TABLE orders_cdc (
  order_id BIGINT,
  customer_id BIGINT,
  amount DECIMAL(10, 2),
  order_note STRING,
  order_time TIMESTAMP(3),
  WATERMARK FOR order_time AS order_time - INTERVAL '0' SECOND
) WITH (
  'connector' = 'iceberg-cdc',
  'catalog-impl' = 'org.apache.iceberg.hadoop.HadoopCatalog',
  'warehouse' = 'file:///tmp/mat-flink-sql-cdc-e2e/warehouse',
  'table' = 'e2e.orders',
  'discovery-interval' = '200 ms',
  'scan.startup.mode' = 'latest',
  'bounded' = 'false',
  'split-assignment-mode' = 'ordered'
);

CREATE TEMPORARY TABLE customer_events_cdc (
  customer_id BIGINT NOT NULL,
  customer_name STRING,
  tier STRING,
  is_deleted BOOLEAN,
  change_time TIMESTAMP(3),
  WATERMARK FOR change_time AS change_time - INTERVAL '0' SECOND,
  PRIMARY KEY (customer_id) NOT ENFORCED
) WITH (
  'connector' = 'iceberg-cdc',
  'catalog-impl' = 'org.apache.iceberg.hadoop.HadoopCatalog',
  'warehouse' = 'file:///tmp/mat-flink-sql-cdc-e2e/warehouse',
  'table' = 'e2e.customer_events',
  'discovery-interval' = '200 ms',
  'scan.startup.mode' = 'latest',
  'bounded' = 'false',
  'split-assignment-mode' = 'ordered'
);

SELECT
  o.order_id,
  o.customer_id,
  o.amount,
  CASE
    WHEN COALESCE(c.is_deleted, TRUE) = FALSE THEN c.customer_name
    ELSE CAST(NULL AS STRING)
  END AS customer_name,
  CASE
    WHEN COALESCE(c.is_deleted, TRUE) = FALSE THEN c.tier
    ELSE CAST(NULL AS STRING)
  END AS tier,
  COALESCE(c.is_deleted = FALSE, FALSE) AS matched_customer
FROM orders_cdc AS o
LEFT JOIN customer_events_cdc FOR SYSTEM_TIME AS OF o.order_time AS c
ON o.customer_id = c.customer_id
WHERE o.order_id > 0;
