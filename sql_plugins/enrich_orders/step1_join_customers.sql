-- Enrich with customer data, store in temp table
SELECT 
  @cdc_lsn as cdc_lsn,
  @cdc_tx_id as cdc_transaction_id,
  @cdc_table as cdc_source_table,
  @cdc_operation as cdc_operation,
  @order_id as order_id,
  @customer_id as customer_id,
  c.name as customer_name,
  c.email as customer_email,
  c.segment as customer_segment
INTO #customers_enriched
FROM (SELECT @customer_id as customer_id, @order_id as order_id) o
LEFT JOIN customers c ON o.customer_id = c.id;
