-- Fanout: Create two views from enriched result

-- View 1: customers_view
SELECT 
  cdc_lsn,
  cdc_transaction_id,
  cdc_source_table,
  cdc_operation,
  customer_id,
  customer_name,
  customer_email,
  customer_segment
FROM #enriched
WHERE customer_id IS NOT NULL;

-- View 2: products_view
SELECT 
  cdc_lsn,
  cdc_transaction_id,
  cdc_source_table,
  cdc_operation,
  product_id,
  product_name,
  product_category,
  product_price
FROM #enriched
WHERE product_id IS NOT NULL;
