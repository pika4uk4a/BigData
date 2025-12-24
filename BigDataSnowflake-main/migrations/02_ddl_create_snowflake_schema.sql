-- DDL: Создание схемы снежинки

-- =============
-- + ИЗМЕРЕНИЯ +
-- =============

-- Измерение: Поставщики
CREATE TABLE dim_supplier (
    supplier_id SERIAL PRIMARY KEY,
    supplier_name VARCHAR(255) NOT NULL,
    supplier_contact VARCHAR(255),
    supplier_email VARCHAR(255),
    supplier_phone VARCHAR(255),
    supplier_address VARCHAR(255),
    supplier_city VARCHAR(255),
    supplier_country VARCHAR(255),
    CONSTRAINT uk_supplier_name UNIQUE (supplier_name)
);

-- Измерение: Покупатели
CREATE TABLE dim_customer (
    customer_id SERIAL PRIMARY KEY,
    customer_first_name VARCHAR(255),
    customer_last_name VARCHAR(255),
    customer_age INTEGER,
    customer_email VARCHAR(255) NOT NULL,
    customer_country VARCHAR(255),
    customer_postal_code VARCHAR(255),
    customer_pet_type VARCHAR(255),
    customer_pet_name VARCHAR(255),
    customer_pet_breed VARCHAR(255),
    CONSTRAINT uk_customer_email UNIQUE (customer_email)
);

-- Измерение: Продавцы
CREATE TABLE dim_seller (
    seller_id SERIAL PRIMARY KEY,
    seller_first_name VARCHAR(255),
    seller_last_name VARCHAR(255),
    seller_email VARCHAR(255) NOT NULL,
    seller_country VARCHAR(255),
    seller_postal_code VARCHAR(255),
    CONSTRAINT uk_seller_email UNIQUE (seller_email)
);

-- Измерение: Магазины
CREATE TABLE dim_store (
    store_id SERIAL PRIMARY KEY,
    store_name VARCHAR(255) NOT NULL,
    store_location VARCHAR(255),
    store_city VARCHAR(255),
    store_state VARCHAR(255),
    store_country VARCHAR(255),
    store_phone VARCHAR(255),
    store_email VARCHAR(255),
    CONSTRAINT uk_store_name UNIQUE (store_name)
);

-- Измерение: Товары
CREATE TABLE dim_product (
    product_id SERIAL PRIMARY KEY,
    product_name VARCHAR(255) NOT NULL,
    product_category VARCHAR(255),
    product_price DECIMAL(10, 2),
    product_quantity INTEGER,
    pet_category VARCHAR(255),
    product_weight DECIMAL(10, 2),
    product_color VARCHAR(255),
    product_size VARCHAR(255),
    product_brand VARCHAR(255),
    product_material VARCHAR(255),
    product_description TEXT,
    product_rating DECIMAL(3, 1),
    product_reviews INTEGER,
    product_release_date VARCHAR(255),
    product_expiry_date VARCHAR(255),
    supplier_id INTEGER,
    CONSTRAINT fk_product_supplier FOREIGN KEY (supplier_id) REFERENCES dim_supplier(supplier_id),
    CONSTRAINT uk_product_name UNIQUE (product_name)
);

-- Измерение: Дата
CREATE TABLE dim_date (
    date_id SERIAL PRIMARY KEY,
    sale_date DATE NOT NULL UNIQUE,
    day_of_week INTEGER,
    day_name VARCHAR(20),
    month INTEGER,
    month_name VARCHAR(20),
    quarter INTEGER,
    year INTEGER,
    is_weekend BOOLEAN
);

-- =========
-- + ФАКТЫ +
-- =========

-- Факт: Продажи
CREATE TABLE fact_sales (
    sale_id SERIAL PRIMARY KEY,
    sale_date DATE NOT NULL,
    date_id INTEGER,
    customer_id INTEGER NOT NULL,
    seller_id INTEGER NOT NULL,
    product_id INTEGER NOT NULL,
    store_id INTEGER NOT NULL,
    sale_quantity INTEGER NOT NULL,
    sale_total_price DECIMAL(10, 2) NOT NULL,
    CONSTRAINT fk_sales_date FOREIGN KEY (date_id) REFERENCES dim_date(date_id),
    CONSTRAINT fk_sales_customer FOREIGN KEY (customer_id) REFERENCES dim_customer(customer_id),
    CONSTRAINT fk_sales_seller FOREIGN KEY (seller_id) REFERENCES dim_seller(seller_id),
    CONSTRAINT fk_sales_product FOREIGN KEY (product_id) REFERENCES dim_product(product_id),
    CONSTRAINT fk_sales_store FOREIGN KEY (store_id) REFERENCES dim_store(store_id)
);

-- Индексы
CREATE INDEX idx_fact_sales_date ON fact_sales(sale_date);
CREATE INDEX idx_fact_sales_customer ON fact_sales(customer_id);
CREATE INDEX idx_fact_sales_product ON fact_sales(product_id);
CREATE INDEX idx_fact_sales_store ON fact_sales(store_id);
