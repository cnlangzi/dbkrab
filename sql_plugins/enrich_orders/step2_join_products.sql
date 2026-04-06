-- Continue with products enrichment, final result to #products_enriched
WITH base AS (
  SELECT * FROM customers_enriched
)
SELECT 
  b.cdc_lsn,
  b.cdc_transaction_id,
  b.cdc_source_table,
  b.cdc_operation,
  b.order_id,
  b.customer_id,
  b.customer_name,
  b.customer_email,
  b.customer_segment,
  p.product_name,
  p.category as product_category,
  p.price as product_price
INTO #products_enriched
FROM base b
LEFT JOIN products p ON b.order_id = p.order_id;
