-- Aggregation 1: Geography & Finances
CREATE TABLE IF NOT EXISTS geo_analytics (
    customer_state VARCHAR(10) PRIMARY KEY,
    total_orders INT,
    average_payment NUMERIC(10, 2)
);

-- Aggregation 2: Quality & Products
CREATE TABLE IF NOT EXISTS product_reviews_analytics (
    product_category_name VARCHAR(100) PRIMARY KEY,
    total_reviews INT,
    average_rating NUMERIC(3, 2)
);

-- Aggregation 3: Logistics & Delivery
CREATE TABLE IF NOT EXISTS logistics_delivery_analytics (
    id SERIAL PRIMARY KEY,
    average_shipping_days NUMERIC(5, 2)
);

-- Aggregation 4: Payment Methods & Risk
CREATE TABLE IF NOT EXISTS payment_type_analytics (
    payment_type VARCHAR(50) PRIMARY KEY,
    transactions INT,
    average_amount NUMERIC(10, 2)
);