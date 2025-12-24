-- DML: Заполнение таблиц фактов и измерений из исходных данных

-- ========================
-- + ЗАПОЛНЕНИЕ ИЗМЕРЕНИЙ +
-- ========================

-- 1. Заполнение dim_supplier
INSERT INTO dim_supplier (
    supplier_name,
    supplier_contact,
    supplier_email,
    supplier_phone,
    supplier_address,
    supplier_city,
    supplier_country
)
SELECT DISTINCT ON (supplier_name)
    supplier_name,
    supplier_contact,
    supplier_email,
    supplier_phone,
    supplier_address,
    supplier_city,
    supplier_country
FROM mock_data
WHERE supplier_name IS NOT NULL
ORDER BY supplier_name, supplier_email NULLS LAST;

-- 2. Заполнение dim_customer
INSERT INTO dim_customer (
    customer_first_name,
    customer_last_name,
    customer_age,
    customer_email,
    customer_country,
    customer_postal_code,
    customer_pet_type,
    customer_pet_name,
    customer_pet_breed
)
SELECT DISTINCT ON (customer_email)
    customer_first_name,
    customer_last_name,
    customer_age,
    customer_email,
    customer_country,
    customer_postal_code,
    customer_pet_type,
    customer_pet_name,
    customer_pet_breed
FROM mock_data
WHERE customer_email IS NOT NULL
ORDER BY customer_email, customer_first_name NULLS LAST;

-- 3. Заполнение dim_seller
INSERT INTO dim_seller (
    seller_first_name,
    seller_last_name,
    seller_email,
    seller_country,
    seller_postal_code
)
SELECT DISTINCT ON (seller_email)
    seller_first_name,
    seller_last_name,
    seller_email,
    seller_country,
    seller_postal_code
FROM mock_data
WHERE seller_email IS NOT NULL
ORDER BY seller_email, seller_first_name NULLS LAST;

-- 4. Заполнение dim_store
INSERT INTO dim_store (
    store_name,
    store_location,
    store_city,
    store_state,
    store_country,
    store_phone,
    store_email
)
SELECT DISTINCT ON (store_name)
    store_name,
    store_location,
    store_city,
    store_state,
    store_country,
    store_phone,
    store_email
FROM mock_data
WHERE store_name IS NOT NULL
ORDER BY store_name, store_city NULLS LAST;

-- 5. Заполнение dim_product
INSERT INTO dim_product (
    product_name,
    product_category,
    product_price,
    product_quantity,
    pet_category,
    product_weight,
    product_color,
    product_size,
    product_brand,
    product_material,
    product_description,
    product_rating,
    product_reviews,
    product_release_date,
    product_expiry_date,
    supplier_id
)
SELECT DISTINCT ON (m.product_name)
    m.product_name,
    m.product_category,
    m.product_price,
    m.product_quantity,
    m.pet_category,
    m.product_weight,
    m.product_color,
    m.product_size,
    m.product_brand,
    m.product_material,
    m.product_description,
    m.product_rating,
    m.product_reviews,
    m.product_release_date,
    m.product_expiry_date,
    s.supplier_id
FROM mock_data m
LEFT JOIN dim_supplier s ON m.supplier_name = s.supplier_name
WHERE m.product_name IS NOT NULL
ORDER BY m.product_name, m.product_price NULLS LAST;

-- 6. Заполнение dim_date
INSERT INTO dim_date (
    sale_date,
    day_of_week,
    day_name,
    month,
    month_name,
    quarter,
    year,
    is_weekend
)
SELECT DISTINCT
    TO_DATE(sale_date, 'FMMM/FMDD/YYYY') as sale_date,
    EXTRACT(DOW FROM TO_DATE(sale_date, 'FMMM/FMDD/YYYY'))::INTEGER as day_of_week,
    TRIM(TO_CHAR(TO_DATE(sale_date, 'FMMM/FMDD/YYYY'), 'Day')) as day_name,
    EXTRACT(MONTH FROM TO_DATE(sale_date, 'FMMM/FMDD/YYYY'))::INTEGER as month,
    TRIM(TO_CHAR(TO_DATE(sale_date, 'FMMM/FMDD/YYYY'), 'Month')) as month_name,
    EXTRACT(QUARTER FROM TO_DATE(sale_date, 'FMMM/FMDD/YYYY'))::INTEGER as quarter,
    EXTRACT(YEAR FROM TO_DATE(sale_date, 'FMMM/FMDD/YYYY'))::INTEGER as year,
    EXTRACT(DOW FROM TO_DATE(sale_date, 'FMMM/FMDD/YYYY')) IN (0, 6) as is_weekend
FROM mock_data
WHERE sale_date IS NOT NULL
ORDER BY TO_DATE(sale_date, 'FMMM/FMDD/YYYY');

-- =====================
-- + ЗАПОЛНЕНИЕ ФАКТОВ +
-- =====================

-- 1. Заполнение fact_sales
INSERT INTO fact_sales (
    sale_date,
    date_id,
    customer_id,
    seller_id,
    product_id,
    store_id,
    sale_quantity,
    sale_total_price
)
SELECT
    TO_DATE(m.sale_date, 'FMMM/FMDD/YYYY') as sale_date,
    d.date_id,
    c.customer_id,
    s.seller_id,
    p.product_id,
    st.store_id,
    m.sale_quantity,
    m.sale_total_price
FROM mock_data m
INNER JOIN dim_customer c ON m.customer_email = c.customer_email
INNER JOIN dim_seller s ON m.seller_email = s.seller_email
INNER JOIN dim_product p ON m.product_name = p.product_name
INNER JOIN dim_store st ON m.store_name = st.store_name
INNER JOIN dim_date d ON TO_DATE(m.sale_date, 'FMMM/FMDD/YYYY') = d.sale_date
WHERE m.sale_date IS NOT NULL
  AND m.customer_email IS NOT NULL
  AND m.seller_email IS NOT NULL
  AND m.product_name IS NOT NULL
  AND m.store_name IS NOT NULL
ORDER BY TO_DATE(m.sale_date, 'FMMM/FMDD/YYYY'), m.id;
