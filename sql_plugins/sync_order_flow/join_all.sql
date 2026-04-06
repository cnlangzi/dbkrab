-- Handle CDC batch with multiple tables
-- CDC batch data is written to #cdc_batch by Engine

SELECT 
  b.cdc_lsn,
  b.cdc_transaction_id,
  b.cdc_source_table,
  b.cdc_operation,
  b.order_id,
  -- orders fields (NULL for order_items)
  b.amount,
  -- order_items fields (NULL for orders)
  b.quantity,
  -- Joined customer data
  c.name as customer_name,
  c.email as customer_email,
  c.segment as customer_segment,
  -- Joined product data
  p.product_name,
  p.category as product_category,
  p.price as product_price,
  -- Computed amount
  CASE 
    WHEN b.cdc_source_table = 'order_items' THEN p.price * b.quantity
    ELSE b.amount
  END as final_amount
INTO #enriched_flow
FROM #cdc_batch b
LEFT JOIN customers c ON b.customer_id = c.id
LEFT JOIN products p ON b.product_id = p.id;
