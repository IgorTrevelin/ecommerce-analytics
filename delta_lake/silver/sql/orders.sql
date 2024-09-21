SELECT
  order_id AS order_id,
  customer_id AS customer_id,
  status AS status,
  DATETIME(purchase_timestamp) AS purchase_timestamp,
  DATETIME(approved_at) AS approved_at,
  DATETIME(delivered_carrier_date) AS delivered_carrier_date,
  DATETIME(delivered_customer_date) AS delivered_customer_date,
  DATETIME(estimated_delivery_date) AS estimated_delivery_date
FROM {df}