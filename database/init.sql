CREATE EXTENSION IF NOT EXISTS pgcrypto;

DROP SCHEMA IF EXISTS raw CASCADE;
DROP SCHEMA IF EXISTS anonymized CASCADE;

CREATE SCHEMA raw;
CREATE SCHEMA anonymized;

CREATE TABLE raw.customers (
    customer_id TEXT,
    customer_unique_id TEXT,
    customer_zip_code_prefix INTEGER,
    customer_city TEXT,
    customer_state TEXT
);

CREATE TABLE raw.sellers (
    seller_id TEXT,
    seller_zip_code_prefix INTEGER,
    seller_city TEXT,
    seller_state TEXT
);

CREATE TABLE raw.products (
    product_id TEXT,
    product_category_name TEXT,
    product_name_lenght INTEGER,
    product_description_lenght INTEGER,
    product_photos_qty INTEGER,
    product_weight_g INTEGER,
    product_length_cm INTEGER,
    product_height_cm INTEGER,
    product_width_cm INTEGER
);

CREATE TABLE raw.product_category_translation (
    product_category_name TEXT,
    product_category_name_english TEXT
);

CREATE TABLE raw.orders (
    order_id TEXT,
    customer_id TEXT,
    order_status TEXT,
    order_purchase_timestamp TIMESTAMP,
    order_approved_at TIMESTAMP,
    order_delivered_carrier_date TIMESTAMP,
    order_delivered_customer_date TIMESTAMP,
    order_estimated_delivery_date TIMESTAMP
);

CREATE TABLE raw.order_items (
    order_id TEXT,
    order_item_id INTEGER,
    product_id TEXT,
    seller_id TEXT,
    shipping_limit_date TIMESTAMP,
    price NUMERIC(12,2),
    freight_value NUMERIC(12,2)
);

CREATE TABLE raw.order_payments (
    order_id TEXT,
    payment_sequential INTEGER,
    payment_type TEXT,
    payment_installments INTEGER,
    payment_value NUMERIC(12,2)
);

CREATE TABLE raw.order_reviews (
    review_id TEXT,
    order_id TEXT,
    review_score INTEGER,
    review_comment_title TEXT,
    review_comment_message TEXT,
    review_creation_date TIMESTAMP,
    review_answer_timestamp TIMESTAMP
);

CREATE TABLE raw.geolocation (
    geolocation_zip_code_prefix INTEGER,
    geolocation_lat NUMERIC,
    geolocation_lng NUMERIC,
    geolocation_city TEXT,
    geolocation_state TEXT
);

SELECT 'customers' AS tabla, COUNT(*) FROM raw.customers
UNION ALL
SELECT 'sellers', COUNT(*) FROM raw.sellers
UNION ALL
SELECT 'products', COUNT(*) FROM raw.products
UNION ALL
SELECT 'orders', COUNT(*) FROM raw.orders
UNION ALL
SELECT 'order_items', COUNT(*) FROM raw.order_items
UNION ALL
SELECT 'order_payments', COUNT(*) FROM raw.order_payments
UNION ALL
SELECT 'order_reviews', COUNT(*) FROM raw.order_reviews
UNION ALL
SELECT 'geolocation', COUNT(*) FROM raw.geolocation;

CREATE OR REPLACE FUNCTION anonymized.hash_id(value TEXT)
RETURNS TEXT AS $$
    SELECT encode(
        digest(COALESCE(value, '') || 'SALT_PROYECTO_2026_OLIST', 'sha256'),
        'hex'
    );
$$ LANGUAGE SQL IMMUTABLE;

CREATE OR REPLACE VIEW anonymized.customers AS
SELECT
    anonymized.hash_id(customer_id) AS customer_id_hash,
    anonymized.hash_id(customer_unique_id) AS customer_unique_id_hash,
    customer_state,
    customer_city
FROM raw.customers;

CREATE OR REPLACE VIEW anonymized.sellers AS
SELECT
    anonymized.hash_id(seller_id) AS seller_id_hash,
    seller_state,
    seller_city
FROM raw.sellers;

CREATE OR REPLACE VIEW anonymized.products AS
SELECT
    anonymized.hash_id(product_id) AS product_id_hash,
    product_category_name,
    product_photos_qty,
    product_weight_g,
    product_length_cm,
    product_height_cm,
    product_width_cm,

    CASE
        WHEN product_weight_g IS NULL THEN NULL
        WHEN product_weight_g < 500 THEN 'very_light'
        WHEN product_weight_g < 2000 THEN 'light'
        WHEN product_weight_g < 5000 THEN 'medium'
        WHEN product_weight_g < 15000 THEN 'heavy'
        ELSE 'very_heavy'
    END AS product_weight_range
FROM raw.products;

CREATE OR REPLACE VIEW anonymized.product_category_translation AS
SELECT
    product_category_name,
    product_category_name_english
FROM raw.product_category_translation;

CREATE OR REPLACE VIEW anonymized.orders AS
SELECT
    anonymized.hash_id(order_id) AS order_id_hash,
    anonymized.hash_id(customer_id) AS customer_id_hash,
    order_status,

    DATE_TRUNC('month', order_purchase_timestamp)::DATE AS purchase_month,
    DATE_TRUNC('month', order_approved_at)::DATE AS approved_month,
    DATE_TRUNC('month', order_delivered_customer_date)::DATE AS delivered_month,
    DATE_TRUNC('month', order_estimated_delivery_date)::DATE AS estimated_delivery_month,

    CASE
        WHEN order_delivered_customer_date IS NOT NULL
         AND order_estimated_delivery_date IS NOT NULL
         AND order_delivered_customer_date <= order_estimated_delivery_date
        THEN true
        WHEN order_delivered_customer_date IS NOT NULL
         AND order_estimated_delivery_date IS NOT NULL
        THEN false
        ELSE NULL
    END AS delivered_on_time,

    CASE
        WHEN order_delivered_customer_date IS NOT NULL
         AND order_purchase_timestamp IS NOT NULL
        THEN DATE_PART('day', order_delivered_customer_date - order_purchase_timestamp)::INTEGER
        ELSE NULL
    END AS delivery_days,

    CASE
        WHEN order_delivered_customer_date IS NULL
          OR order_purchase_timestamp IS NULL THEN NULL
        WHEN DATE_PART('day', order_delivered_customer_date - order_purchase_timestamp) <= 3 THEN '0-3_days'
        WHEN DATE_PART('day', order_delivered_customer_date - order_purchase_timestamp) <= 7 THEN '4-7_days'
        WHEN DATE_PART('day', order_delivered_customer_date - order_purchase_timestamp) <= 15 THEN '8-15_days'
        WHEN DATE_PART('day', order_delivered_customer_date - order_purchase_timestamp) <= 30 THEN '16-30_days'
        ELSE '30+_days'
    END AS delivery_time_range
FROM raw.orders;

CREATE OR REPLACE VIEW anonymized.order_items AS
SELECT
    anonymized.hash_id(order_id) AS order_id_hash,
    order_item_id,
    anonymized.hash_id(product_id) AS product_id_hash,
    anonymized.hash_id(seller_id) AS seller_id_hash,

    DATE_TRUNC('month', shipping_limit_date)::DATE AS shipping_limit_month,

    CASE
        WHEN price IS NULL THEN NULL
        WHEN price < 50 THEN '0-49'
        WHEN price < 100 THEN '50-99'
        WHEN price < 250 THEN '100-249'
        WHEN price < 500 THEN '250-499'
        WHEN price < 1000 THEN '500-999'
        ELSE '1000+'
    END AS price_range,

    CASE
        WHEN freight_value IS NULL THEN NULL
        WHEN freight_value < 20 THEN '0-19'
        WHEN freight_value < 50 THEN '20-49'
        WHEN freight_value < 100 THEN '50-99'
        ELSE '100+'
    END AS freight_range,

    CASE
        WHEN price IS NULL OR freight_value IS NULL THEN NULL
        WHEN price = 0 THEN NULL
        ELSE ROUND((freight_value / price) * 100, 1)
    END AS freight_percentage_of_price
FROM raw.order_items;

CREATE OR REPLACE VIEW anonymized.order_payments AS
SELECT
    anonymized.hash_id(order_id) AS order_id_hash,
    payment_sequential,
    payment_type,
    payment_installments,

    CASE
        WHEN payment_value IS NULL THEN NULL
        WHEN payment_value < 50 THEN '0-49'
        WHEN payment_value < 100 THEN '50-99'
        WHEN payment_value < 250 THEN '100-249'
        WHEN payment_value < 500 THEN '250-499'
        WHEN payment_value < 1000 THEN '500-999'
        ELSE '1000+'
    END AS payment_value_range,

    CASE
        WHEN payment_installments IS NULL THEN NULL
        WHEN payment_installments = 1 THEN 'single_payment'
        WHEN payment_installments BETWEEN 2 AND 3 THEN '2-3_installments'
        WHEN payment_installments BETWEEN 4 AND 6 THEN '4-6_installments'
        WHEN payment_installments BETWEEN 7 AND 12 THEN '7-12_installments'
        ELSE '13+_installments'
    END AS installment_range
FROM raw.order_payments;

CREATE OR REPLACE VIEW anonymized.order_reviews AS
SELECT
    anonymized.hash_id(review_id) AS review_id_hash,
    anonymized.hash_id(order_id) AS order_id_hash,
    review_score,

    CASE
        WHEN review_comment_title IS NULL OR TRIM(review_comment_title) = '' THEN false
        ELSE true
    END AS has_review_title,

    CASE
        WHEN review_comment_message IS NULL OR TRIM(review_comment_message) = '' THEN false
        ELSE true
    END AS has_review_comment,

    CASE
        WHEN review_score IS NULL THEN NULL
        WHEN review_score <= 2 THEN 'negative'
        WHEN review_score = 3 THEN 'neutral'
        WHEN review_score >= 4 THEN 'positive'
    END AS review_sentiment_category,

    DATE_TRUNC('month', review_creation_date)::DATE AS review_creation_month,
    DATE_TRUNC('month', review_answer_timestamp)::DATE AS review_answer_month
FROM raw.order_reviews;

CREATE OR REPLACE VIEW anonymized.ecommerce_orders_summary AS
SELECT
    o.order_id_hash,
    o.customer_id_hash,
    c.customer_state,
    c.customer_city,

    o.order_status,
    o.purchase_month,
    o.approved_month,
    o.delivered_month,
    o.estimated_delivery_month,
    o.delivered_on_time,
    o.delivery_days,
    o.delivery_time_range,

    oi.order_item_id,
    oi.product_id_hash,
    p.product_category_name,
    pct.product_category_name_english,
    p.product_photos_qty,
    p.product_weight_range,

    oi.seller_id_hash,
    s.seller_state,
    s.seller_city,

    oi.price_range,
    oi.freight_range,
    oi.freight_percentage_of_price,

    pay.payment_type,
    pay.installment_range,
    pay.payment_value_range,

    r.review_score,
    r.review_sentiment_category,
    r.has_review_title,
    r.has_review_comment,
    r.review_creation_month
FROM anonymized.orders o
LEFT JOIN anonymized.customers c
    ON o.customer_id_hash = c.customer_id_hash
LEFT JOIN anonymized.order_items oi
    ON o.order_id_hash = oi.order_id_hash
LEFT JOIN anonymized.products p
    ON oi.product_id_hash = p.product_id_hash
LEFT JOIN anonymized.product_category_translation pct
    ON p.product_category_name = pct.product_category_name
LEFT JOIN anonymized.sellers s
    ON oi.seller_id_hash = s.seller_id_hash
LEFT JOIN anonymized.order_payments pay
    ON o.order_id_hash = pay.order_id_hash
LEFT JOIN anonymized.order_reviews r
    ON o.order_id_hash = r.order_id_hash;

SELECT *
FROM anonymized.ecommerce_orders_summary
LIMIT 20;

SELECT COUNT(*) AS total_rows
FROM anonymized.ecommerce_orders_summary;


SELECT
    customer_id_hash,
    customer_state,
    customer_city
FROM anonymized.customers
LIMIT 20;