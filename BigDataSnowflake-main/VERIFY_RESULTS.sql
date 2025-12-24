-- Скрипт проверки результатов нормализации (пункт 10)
-- Упрощенная версия для быстрого выполнения

-- ===============================
-- + ПРОВЕРКА КОЛИЧЕСТВА ЗАПИСЕЙ +
-- ===============================

-- Исходные данные
SELECT 
    'Исходные данные (mock_data)' as table_name,
    COUNT(*) as row_count
FROM mock_data;

-- Измерения и факты - отдельными запросами для надежности
SELECT 'dim_supplier' as table_name, COUNT(*) as row_count FROM dim_supplier;
SELECT 'dim_customer' as table_name, COUNT(*) as row_count FROM dim_customer;
SELECT 'dim_seller' as table_name, COUNT(*) as row_count FROM dim_seller;
SELECT 'dim_store' as table_name, COUNT(*) as row_count FROM dim_store;
SELECT 'dim_product' as table_name, COUNT(*) as row_count FROM dim_product;
SELECT 'dim_date' as table_name, COUNT(*) as row_count FROM dim_date;
SELECT 'fact_sales' as table_name, COUNT(*) as row_count FROM fact_sales;

-- ===============================
-- + ПРОВЕРКА ЦЕЛОСТНОСТИ ДАННЫХ +
-- ===============================

-- Проверка: все продажи имеют корректные связи
SELECT 
    'Продажи без покупателя' as check_name,
    COUNT(*) as issue_count
FROM fact_sales fs
LEFT JOIN dim_customer c ON fs.customer_id = c.customer_id
WHERE c.customer_id IS NULL;

SELECT 
    'Продажи без продавца' as check_name,
    COUNT(*) as issue_count
FROM fact_sales fs
LEFT JOIN dim_seller s ON fs.seller_id = s.seller_id
WHERE s.seller_id IS NULL;

SELECT 
    'Продажи без товара' as check_name,
    COUNT(*) as issue_count
FROM fact_sales fs
LEFT JOIN dim_product p ON fs.product_id = p.product_id
WHERE p.product_id IS NULL;

SELECT 
    'Продажи без магазина' as check_name,
    COUNT(*) as issue_count
FROM fact_sales fs
LEFT JOIN dim_store st ON fs.store_id = st.store_id
WHERE st.store_id IS NULL;

SELECT 
    'Товары без поставщика' as check_name,
    COUNT(*) as issue_count
FROM dim_product p
WHERE p.supplier_id IS NULL;

-- ================================
-- + ПРОВЕРКА СООТВЕТСТВИЯ ДАННЫХ +
-- ================================

-- Используем CTE для оптимизации
WITH source_counts AS (
    SELECT 
        COUNT(DISTINCT customer_email) as unique_customers,
        COUNT(DISTINCT seller_email) as unique_sellers,
        COUNT(DISTINCT product_name) as unique_products,
        COUNT(DISTINCT store_name) as unique_stores
    FROM mock_data
    WHERE customer_email IS NOT NULL 
      AND seller_email IS NOT NULL 
      AND product_name IS NOT NULL 
      AND store_name IS NOT NULL
),
dimension_counts AS (
    SELECT 
        (SELECT COUNT(*) FROM dim_customer) as dim_customers,
        (SELECT COUNT(*) FROM dim_seller) as dim_sellers,
        (SELECT COUNT(*) FROM dim_product) as dim_products,
        (SELECT COUNT(*) FROM dim_store) as dim_stores
)
SELECT 
    'Уникальные покупатели' as check_name,
    sc.unique_customers as source_count,
    dc.dim_customers as dimension_count,
    CASE WHEN sc.unique_customers = dc.dim_customers THEN 'OK' ELSE 'MISMATCH' END as status
FROM source_counts sc, dimension_counts dc

UNION ALL

SELECT 
    'Уникальные продавцы',
    sc.unique_sellers,
    dc.dim_sellers,
    CASE WHEN sc.unique_sellers = dc.dim_sellers THEN 'OK' ELSE 'MISMATCH' END
FROM source_counts sc, dimension_counts dc

UNION ALL

SELECT 
    'Уникальные товары',
    sc.unique_products,
    dc.dim_products,
    CASE WHEN sc.unique_products = dc.dim_products THEN 'OK' ELSE 'MISMATCH' END
FROM source_counts sc, dimension_counts dc

UNION ALL

SELECT 
    'Уникальные магазины',
    sc.unique_stores,
    dc.dim_stores,
    CASE WHEN sc.unique_stores = dc.dim_stores THEN 'OK' ELSE 'MISMATCH' END
FROM source_counts sc, dimension_counts dc;

-- ==========================
-- + ПРОВЕРКА СУММ И МЕТРИК +
-- ==========================

-- Сравнение сумм продаж
SELECT 
    'Сумма продаж в исходных данных' as check_name,
    SUM(sale_total_price) as total_amount
FROM mock_data
WHERE sale_total_price IS NOT NULL;

SELECT 
    'Сумма продаж в fact_sales' as check_name,
    SUM(sale_total_price) as total_amount
FROM fact_sales;

-- Сравнение количества продаж
SELECT 
    'Количество продаж в исходных данных' as check_name,
    COUNT(*) as sale_count
FROM mock_data
WHERE sale_date IS NOT NULL;

SELECT 
    'Количество продаж в fact_sales' as check_name,
    COUNT(*) as sale_count
FROM fact_sales;

-- ==========================
-- + АНАЛИТИЧЕСКИЕ ЗАПРОСЫ +
-- ==========================

-- Топ-10 покупателей по сумме покупок
SELECT 
    c.customer_first_name || ' ' || c.customer_last_name as customer_name,
    c.customer_country,
    COUNT(fs.sale_id) as purchase_count,
    SUM(fs.sale_total_price) as total_spent
FROM fact_sales fs
JOIN dim_customer c ON fs.customer_id = c.customer_id
GROUP BY c.customer_id, c.customer_first_name, c.customer_last_name, c.customer_country
ORDER BY total_spent DESC
LIMIT 10;

-- Топ-10 товаров по продажам
SELECT 
    p.product_name,
    p.product_category,
    COUNT(fs.sale_id) as sale_count,
    SUM(fs.sale_quantity) as total_quantity,
    SUM(fs.sale_total_price) as total_revenue
FROM fact_sales fs
JOIN dim_product p ON fs.product_id = p.product_id
GROUP BY p.product_id, p.product_name, p.product_category
ORDER BY total_revenue DESC
LIMIT 10;

-- Продажи по странам магазинов
SELECT 
    st.store_country,
    COUNT(fs.sale_id) as sale_count,
    SUM(fs.sale_total_price) as total_revenue
FROM fact_sales fs
JOIN dim_store st ON fs.store_id = st.store_id
GROUP BY st.store_country
ORDER BY total_revenue DESC
LIMIT 20;

-- Продажи по месяцам
SELECT 
    d.year,
    d.month,
    TRIM(d.month_name) as month_name,
    COUNT(fs.sale_id) as sale_count,
    SUM(fs.sale_total_price) as total_revenue
FROM fact_sales fs
JOIN dim_date d ON fs.date_id = d.date_id
GROUP BY d.year, d.month, d.month_name
ORDER BY d.year, d.month;

-- ===================  
-- + ИТОГОВАЯ СВОДКА +
-- ===================

SELECT 
    'Всего записей в исходных данных' as metric,
    COUNT(*)::TEXT as value
FROM mock_data;

SELECT 
    'Всего записей в fact_sales' as metric,
    COUNT(*)::TEXT as value
FROM fact_sales;

SELECT 
    'Уникальных покупателей' as metric,
    COUNT(*)::TEXT as value
FROM dim_customer;

SELECT 
    'Уникальных товаров' as metric,
    COUNT(*)::TEXT as value
FROM dim_product;

SELECT 
    'Уникальных магазинов' as metric,
    COUNT(*)::TEXT as value
FROM dim_store;

SELECT 
    'Общая сумма продаж' as metric,
    TO_CHAR(SUM(sale_total_price), '999,999,999.99') as value
FROM fact_sales;

-- Проверка завершена
