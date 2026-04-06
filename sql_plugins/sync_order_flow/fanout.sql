-- Fanout: Route to appropriate table based on cdc_source_table

-- View 1: orders
SELECT 
  cdc_lsn,
  cdc_transaction_id,
  cdc_source_table,
  cdc_operation,
  order_id,
  amount as order_amount,
  customer_name,
  customer_email,
  customer_segment,
  final_amount
FROM #enriched_flow
WHERE cdc_source_table = 'orders';

-- View 2: order_items
SELECT 
  cdc_lsn,
  cdc_transaction_id,
  cdc_source_table,
  cdc_operation,
  order_id,
  quantity,
  product_name,
  product_category,
  product_price,
  final_amount
FROM #enriched_flow
WHERE cdc_source_table = 'order_items';
