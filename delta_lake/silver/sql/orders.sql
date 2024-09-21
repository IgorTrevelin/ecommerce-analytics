SELECT
  order_id AS order_id,
  customer_id AS customer_id,
  status AS status,
  TIMESTAMP(purchase_timestamp / 1000000) AS purchase_timestamp,
  TIMESTAMP(approved_at / 1000000) AS approved_at,
  TIMESTAMP(delivered_carrier_date / 1000000) AS delivered_carrier_date,
  TIMESTAMP(delivered_customer_date / 1000000) AS delivered_customer_date,
  DATE_FROM_UNIX_DATE(estimated_delivery_date) AS estimated_delivery_date
FROM {df}