-- Enrichment: JOIN customers and products, result to #enriched
SELECT
  @cdc_lsn as cdc_lsn,
  @cdc_tx_id as cdc_transaction_id,
  @cdc_table as cdc_source_table,
  @cdc_operation as cdc_operation,
  @orders_order_id as order_id,
  @orders_customer_id as customer_id,
  @orders_product_id as product_id,
  @orders_amount as order_amount,
  -- Customer fields
  c.name as customer_name,
  c.email as customer_email,
  c.segment as customer_segment,
  -- Product fields
  p.product_name,
  p.category as product_category,
  p.price as product_price
INTO #enriched
FROM (SELECT @orders_customer_id as customer_id, @orders_product_id as product_id, @orders_amount as amount) o
LEFT JOIN customers c ON o.customer_id = c.id
LEFT JOIN products p ON o.product_id = p.id;
