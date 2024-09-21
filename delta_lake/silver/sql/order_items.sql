SELECT
  order_id AS order_id,
  order_item_id AS order_item_id,
  product_id AS product_id,
  seller_id AS seller_id,
  shipping_limit_date AS shipping_limit_date,
  COALESCE(ROUND(price, 2), 0.0) AS price,
  COALESCE(ROUND(freight_value, 2), 0.0) AS freight_value
FROM {df}