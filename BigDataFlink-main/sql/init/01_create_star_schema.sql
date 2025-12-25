CREATE TABLE IF NOT EXISTS dim_customer (
    customer_id INT PRIMARY KEY,
    customer_first_name VARCHAR(255),
    customer_last_name VARCHAR(255),
    customer_email VARCHAR(255),
    customer_country VARCHAR(255)
);

CREATE TABLE IF NOT EXISTS dim_seller (
    seller_id INT PRIMARY KEY,
    seller_first_name VARCHAR(255),
    seller_last_name VARCHAR(255),
    seller_email VARCHAR(255),
    seller_country VARCHAR(255)
);

CREATE TABLE IF NOT EXISTS dim_store (
    store_name VARCHAR(255),
    city VARCHAR(255),
    country VARCHAR(255),
    PRIMARY KEY (store_name, city)
);

CREATE TABLE IF NOT EXISTS dim_product (
    product_id INT PRIMARY KEY,
    product_name VARCHAR(255),
    product_category VARCHAR(255),
    product_price DECIMAL(10, 2)
);

CREATE TABLE IF NOT EXISTS fact_sales (
    sale_id SERIAL PRIMARY KEY,
    customer_id INTEGER NOT NULL,
    seller_id INTEGER NOT NULL,
    product_id INTEGER NOT NULL,
    store_name VARCHAR(255),
    city VARCHAR(255),
    quantity INTEGER,
    total_amount DECIMAL(10, 2),
    sale_date DATE
);

CREATE INDEX IF NOT EXISTS idx_fact_sales_date ON fact_sales(sale_date);
CREATE INDEX IF NOT EXISTS idx_fact_sales_customer ON fact_sales(customer_id);
CREATE INDEX IF NOT EXISTS idx_fact_sales_product ON fact_sales(product_id);
