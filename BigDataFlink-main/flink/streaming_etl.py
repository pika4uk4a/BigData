import os
import logging
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.table import StreamTableEnvironment, EnvironmentSettings

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

KAFKA_BOOTSTRAP_SERVERS = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'kafka:29092')
KAFKA_TOPIC = os.getenv('KAFKA_TOPIC', 'mock_data_topic')
POSTGRES_URL = os.getenv('POSTGRES_URL', 'jdbc:postgresql://postgres:5432/flink_db')
POSTGRES_USER = os.getenv('POSTGRES_USER', 'postgres')
POSTGRES_PASSWORD = os.getenv('POSTGRES_PASSWORD', 'postgres')


def main():
    logger.info("="*50)
    logger.info("Starting Flink Streaming ETL...")
    logger.info("="*50)
    
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(2)
    
    settings = EnvironmentSettings.new_instance().in_streaming_mode().build()
    t_env = StreamTableEnvironment.create(env, environment_settings=settings)
    
    logger.info("Creating Kafka source table...")
    t_env.execute_sql(f"""
        CREATE TABLE IF NOT EXISTS kafka_source (
            id STRING,
            customer_first_name STRING,
            customer_last_name STRING,
            customer_age STRING,
            customer_email STRING,
            customer_country STRING,
            customer_postal_code STRING,
            customer_pet_type STRING,
            customer_pet_name STRING,
            customer_pet_breed STRING,
            seller_first_name STRING,
            seller_last_name STRING,
            seller_email STRING,
            seller_country STRING,
            seller_postal_code STRING,
            product_name STRING,
            product_category STRING,
            product_price STRING,
            product_quantity STRING,
            sale_date STRING,
            sale_customer_id STRING,
            sale_seller_id STRING,
            sale_product_id STRING,
            sale_quantity STRING,
            sale_total_price STRING,
            store_name STRING,
            store_location STRING,
            store_city STRING,
            store_state STRING,
            store_country STRING,
            store_phone STRING,
            store_email STRING,
            pet_category STRING,
            product_weight STRING,
            product_color STRING,
            product_size STRING,
            product_brand STRING,
            product_material STRING,
            product_description STRING,
            product_rating STRING,
            product_reviews STRING,
            product_release_date STRING,
            product_expiry_date STRING,
            supplier_name STRING,
            supplier_contact STRING,
            supplier_email STRING,
            supplier_phone STRING,
            supplier_address STRING,
            supplier_city STRING,
            supplier_country STRING
        ) WITH (
            'connector' = 'kafka',
            'topic' = '{KAFKA_TOPIC}',
            'properties.bootstrap.servers' = '{KAFKA_BOOTSTRAP_SERVERS}',
            'properties.group.id' = 'flink_consumer_group',
            'format' = 'json',
            'scan.startup.mode' = 'earliest-offset'
        )
    """)
    
    logger.info("Creating PostgreSQL sink tables...")
    
    t_env.execute_sql(f"""
        CREATE TABLE IF NOT EXISTS dim_customer_sink (
            customer_id INT,
            customer_first_name STRING,
            customer_last_name STRING,
            customer_email STRING,
            customer_country STRING,
            PRIMARY KEY (customer_id) NOT ENFORCED
        ) WITH (
            'connector' = 'jdbc',
            'url' = '{POSTGRES_URL}',
            'table-name' = 'dim_customer',
            'username' = '{POSTGRES_USER}',
            'password' = '{POSTGRES_PASSWORD}',
            'driver' = 'org.postgresql.Driver'
        )
    """)
    
    t_env.execute_sql(f"""
        CREATE TABLE IF NOT EXISTS dim_product_sink (
            product_id INT,
            product_name STRING,
            product_category STRING,
            product_price DECIMAL(10, 2),
            PRIMARY KEY (product_id) NOT ENFORCED
        ) WITH (
            'connector' = 'jdbc',
            'url' = '{POSTGRES_URL}',
            'table-name' = 'dim_product',
            'username' = '{POSTGRES_USER}',
            'password' = '{POSTGRES_PASSWORD}',
            'driver' = 'org.postgresql.Driver'
        )
    """)
    
    t_env.execute_sql(f"""
        CREATE TABLE IF NOT EXISTS dim_seller_sink (
            seller_id INT,
            seller_first_name STRING,
            seller_last_name STRING,
            seller_email STRING,
            seller_country STRING,
            PRIMARY KEY (seller_id) NOT ENFORCED
        ) WITH (
            'connector' = 'jdbc',
            'url' = '{POSTGRES_URL}',
            'table-name' = 'dim_seller',
            'username' = '{POSTGRES_USER}',
            'password' = '{POSTGRES_PASSWORD}',
            'driver' = 'org.postgresql.Driver'
        )
    """)
    
    t_env.execute_sql(f"""
        CREATE TABLE IF NOT EXISTS dim_store_sink (
            store_name STRING,
            city STRING,
            country STRING,
            PRIMARY KEY (store_name, city) NOT ENFORCED
        ) WITH (
            'connector' = 'jdbc',
            'url' = '{POSTGRES_URL}',
            'table-name' = 'dim_store',
            'username' = '{POSTGRES_USER}',
            'password' = '{POSTGRES_PASSWORD}',
            'driver' = 'org.postgresql.Driver'
        )
    """)
    
    t_env.execute_sql(f"""
        CREATE TABLE IF NOT EXISTS fact_sales_sink (
            customer_id INT,
            seller_id INT,
            product_id INT,
            store_name STRING,
            city STRING,
            quantity INT,
            total_amount DECIMAL(10, 2),
            sale_date DATE
        ) WITH (
            'connector' = 'jdbc',
            'url' = '{POSTGRES_URL}',
            'table-name' = 'fact_sales',
            'username' = '{POSTGRES_USER}',
            'password' = '{POSTGRES_PASSWORD}',
            'driver' = 'org.postgresql.Driver'
        )
    """)
    
    logger.info("Creating statement set for parallel execution...")
    statement_set = t_env.create_statement_set()
    
    logger.info("Adding INSERT statements to statement set...")
    
    logger.info("  - Adding dim_customer INSERT...")
    statement_set.add_insert_sql("""
        INSERT INTO dim_customer_sink
        SELECT DISTINCT
            CAST(sale_customer_id AS INT),
            customer_first_name,
            customer_last_name,
            customer_email,
            customer_country
        FROM kafka_source
        WHERE sale_customer_id IS NOT NULL AND sale_customer_id <> ''
    """)
    
    logger.info("  - Adding dim_product INSERT...")
    statement_set.add_insert_sql("""
        INSERT INTO dim_product_sink
        SELECT DISTINCT
            CAST(sale_product_id AS INT),
            product_name,
            product_category,
            CAST(product_price AS DECIMAL(10, 2))
        FROM kafka_source
        WHERE sale_product_id IS NOT NULL AND sale_product_id <> ''
    """)
    
    logger.info("  - Adding dim_seller INSERT...")
    statement_set.add_insert_sql("""
        INSERT INTO dim_seller_sink
        SELECT DISTINCT
            CAST(sale_seller_id AS INT),
            seller_first_name,
            seller_last_name,
            seller_email,
            seller_country
        FROM kafka_source
        WHERE sale_seller_id IS NOT NULL AND sale_seller_id <> ''
    """)
    
    logger.info("  - Adding dim_store INSERT...")
    statement_set.add_insert_sql("""
        INSERT INTO dim_store_sink
        SELECT DISTINCT
            store_name,
            store_city,
            store_country
        FROM kafka_source
        WHERE store_name IS NOT NULL AND store_name <> ''
    """)
    
    logger.info("  - Adding fact_sales INSERT...")
    statement_set.add_insert_sql("""
        INSERT INTO fact_sales_sink
        SELECT
            CAST(sale_customer_id AS INT),
            CAST(sale_seller_id AS INT),
            CAST(sale_product_id AS INT),
            store_name,
            store_city,
            CAST(sale_quantity AS INT),
            CAST(sale_total_price AS DECIMAL(10, 2)),
            CAST(TO_TIMESTAMP(sale_date, 'M/d/yyyy') AS DATE)
        FROM kafka_source
        WHERE sale_customer_id IS NOT NULL 
            AND sale_seller_id IS NOT NULL 
            AND sale_product_id IS NOT NULL
            AND sale_date IS NOT NULL
    """)
    
    logger.info("="*60)
    logger.info("Executing all INSERT statements in parallel...")
    logger.info("All tables (dimensions + fact) will be processed simultaneously.")
    logger.info("="*60)
    
    result = statement_set.execute()
    
    try:
        job_client = result.get_job_client()
        if job_client:
            job_id = job_client.get_job_id()
            logger.info(f"Job submitted. JobID: {job_id}")
            logger.info(f"Check job status at: http://localhost:8081/#/job/{job_id}")
    except Exception as e:
        logger.warning(f"Could not get job ID: {e}")
    
    logger.info("Waiting for job to complete...")
    logger.info("NOTE: For streaming sources, this may run continuously.")
    result.wait()
    
    logger.info("="*60)
    logger.info("Flink job completed successfully!")
    logger.info("="*60)


if __name__ == '__main__':
    main()
