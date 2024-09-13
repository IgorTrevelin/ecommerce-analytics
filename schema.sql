CREATE TABLE IF NOT EXISTS product_categories (
    category_id SERIAL NOT NULL,
    product_category VARCHAR(50),
    product_category_english VARCHAR(50),

    CONSTRAINT pk_product_category PRIMARY KEY (category_id),
    CONSTRAINT un_product_category_name UNIQUE (product_category)
);

CREATE TABLE IF NOT EXISTS products (
    product_id SERIAL NOT NULL,
    category_id INTEGER NOT NULL,

    CONSTRAINT pk_product PRIMARY KEY (product_id),
    CONSTRAINT fk_product_category 
        FOREIGN key(category_id) REFERENCES product_categories(category_id)
);

CREATE TABLE IF NOT EXISTS zip_code_prefixes (
    zip_code_id SERIAL NOT NULL,
    zip_code_prefix VARCHAR(5) NOT NULL,
    state CHAR(2) NOT NULL,

    CONSTRAINT pk_zip_code_prefix PRIMARY KEY(zip_code_id),
    CONSTRAINT un_zip_code_prefix UNIQUE (zip_code_prefix, state)
);

CREATE TABLE IF NOT EXISTS sellers (
    seller_id SERIAL NOT NULL,
    zip_code_id INTEGER NOT NULL,

    CONSTRAINT pk_seller PRIMARY KEY(seller_id),
    CONSTRAINT fk_seller_zip_code FOREIGN KEY(zip_code_id) REFERENCES zip_code_prefixes(zip_code_id)
);

CREATE TABLE IF NOT EXISTS customers (
    customer_id SERIAL NOT NULL,
    customer_unique_id VARCHAR(32) NOT NULL,
    zip_code_id INTEGER NOT NULL,

    CONSTRAINT pk_customer PRIMARY KEY (customer_id),
    CONSTRAINT fk_customer_zip_code FOREIGN KEY (zip_code_id) REFERENCES zip_code_prefixes(zip_code_id)
);

CREATE TABLE IF NOT EXISTS orders (
    order_id SERIAL NOT NULL,
    customer_id INTEGER NOT NULL,
    status VARCHAR(11) NOT NULL DEFAULT 'created',
    purchase_timestamp TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    approved_at TIMESTAMP NULL,
    delivered_carrier_date TIMESTAMP NULL,
    delivered_customer_date TIMESTAMP NULL,
    estimated_delivery_date DATE NOT NULL,

    CONSTRAINT pk_order PRIMARY KEY (order_id),
    CONSTRAINT fk_order_customer FOREIGN KEY (customer_id) REFERENCES customers(customer_id)
);

CREATE TABLE IF NOT EXISTS order_items (
    order_id INTEGER NOT NULL,
    order_item_id INTEGER NOT NULL,
    product_id INTEGER NOT NULL,
    seller_id INTEGER NOT NULL,
    shipping_limit_date TIMESTAMP NOT NULL,
    price DECIMAL(10, 2) NOT NULL,
    freight_value DECIMAL(10, 2) NOT NULL,

    CONSTRAINT pk_order_item PRIMARY KEY(order_id, order_item_id),
    CONSTRAINT fk_order_item_product FOREIGN KEY (product_id) REFERENCES products(product_id),
    CONSTRAINT fk_order_item_seller FOREIGN KEY (seller_id) REFERENCES sellers(seller_id)
);

CREATE OR REPLACE PROCEDURE sp_create_order_with_items(
    customer_id INTEGER,
    products INTEGER[],
    sellers INTEGER[],
    prices NUMERIC[],
    freight_values NUMERIC[]
)
LANGUAGE plpgsql
AS $$
DECLARE
    i INTEGER;
	new_order_id INTEGER;
BEGIN

	IF array_length(products, 1) != array_length(sellers, 1) OR
       array_length(products, 1) != array_length(prices, 1) OR
       array_length(products, 1) != array_length(freight_values, 1) THEN
        RAISE EXCEPTION 'The provided arrays must be of the same length.';
    END IF;

    --BEGIN

    INSERT INTO orders (customer_id, approved_at, delivered_carrier_date, delivered_customer_date, estimated_delivery_date)
    VALUES (
        customer_id,
        CURRENT_TIMESTAMP + interval '30 seconds',
        CURRENT_TIMESTAMP + interval '1 day',
        CURRENT_TIMESTAMP + interval '3 days',
        CURRENT_TIMESTAMP + interval '3 days'
    )
    RETURNING order_id INTO new_order_id;

    FOR i IN 1..array_length(products, 1) LOOP
        INSERT INTO order_items (order_id, order_item_id, product_id, seller_id, price, freight_value, shipping_limit_date)
        VALUES (new_order_id, i, products[i], sellers[i], prices[i], freight_values[i], CURRENT_TIMESTAMP + interval '1 day');
    END LOOP;
EXCEPTION
    WHEN OTHERS THEN
        RAISE;
-- END;
END;
$$;
