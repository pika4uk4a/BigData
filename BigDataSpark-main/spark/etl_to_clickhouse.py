from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    sum as _sum,
    avg,
    count,
    rank,
    lag,
    concat,
    lit,
)
from pyspark.sql.window import Window

POSTGRES_URL = "jdbc:postgresql://postgres:5432/spark_db"
POSTGRES_PROPERTIES = {
    "user": "postgres",
    "password": "postgres",
    "driver": "org.postgresql.Driver",
}

CLICKHOUSE_URL = "jdbc:clickhouse://clickhouse:8123/spark_marts"
CLICKHOUSE_PROPERTIES = {
    "driver": "com.clickhouse.jdbc.ClickHouseDriver",
    "socket_timeout": "300000",
}


def create_spark_session():
    return SparkSession.builder.appName("ETL to ClickHouse").getOrCreate()


def _read_table(spark, table):
    return (
        spark.read.format("jdbc")
        .option("url", POSTGRES_URL)
        .option("dbtable", table)
        .option("user", POSTGRES_PROPERTIES["user"])
        .option("password", POSTGRES_PROPERTIES["password"])
        .option("driver", POSTGRES_PROPERTIES["driver"])
        .load()
    )


def read_star_schema(spark):
    return {
        "fact_sales": _read_table(spark, "fact_sales"),
        "dim_customer": _read_table(spark, "dim_customer"),
        "dim_product": _read_table(spark, "dim_product"),
        "dim_store": _read_table(spark, "dim_store"),
        "dim_supplier": _read_table(spark, "dim_supplier"),
        "dim_date": _read_table(spark, "dim_date"),
    }


def create_product_sales_mart(fact_sales, dim_product):
    df = fact_sales.join(dim_product, "product_id", "inner")
    agg = (
        df.groupBy(
            "product_id",
            "product_name",
            "product_category",
        )
        .agg(
            _sum("sale_total_price").alias("total_revenue"),
            _sum("sale_quantity").alias("total_quantity"),
            count("*").alias("sale_count"),
            avg("product_rating").alias("avg_rating"),
            _sum("product_reviews").alias("total_reviews"),
        )
    )

    window = Window.orderBy(col("total_revenue").desc())
    return agg.withColumn("rank", rank().over(window))


def create_customer_sales_mart(fact_sales, dim_customer):
    df = fact_sales.join(dim_customer, "customer_id", "inner")
    agg = (
        df.groupBy(
            "customer_id",
            "customer_first_name",
            "customer_last_name",
            "customer_country",
        )
        .agg(
            _sum("sale_total_price").alias("total_spent"),
            count("*").alias("purchase_count"),
            avg("sale_total_price").alias("avg_check"),
        )
        .withColumn(
            "customer_name",
            concat(col("customer_first_name"), lit(" "), col("customer_last_name")),
        )
    )

    window = Window.orderBy(col("total_spent").desc())
    return agg.withColumn("rank", rank().over(window))


def create_time_sales_mart(fact_sales, dim_date):
    df = fact_sales.join(dim_date, fact_sales.date_id == dim_date.date_id, "inner")

    monthly = (
        df.groupBy("year", "month")
        .agg(
            _sum("sale_total_price").alias("total_revenue"),
            count("*").alias("sale_count"),
            avg("sale_total_price").alias("avg_order_size"),
        )
        .orderBy("year", "month")
    )

    window = Window.orderBy("year", "month")
    monthly = monthly.withColumn(
        "prev_month_revenue", lag("total_revenue").over(window)
    ).withColumn(
        "revenue_change", col("total_revenue") - col("prev_month_revenue")
    )

    return monthly


def create_store_sales_mart(fact_sales, dim_store):
    df = fact_sales.join(dim_store, "store_id", "inner")
    agg = (
        df.groupBy("store_id", "store_name", "store_city", "store_country")
        .agg(
            _sum("sale_total_price").alias("total_revenue"),
            count("*").alias("sale_count"),
            avg("sale_total_price").alias("avg_check"),
        )
    )

    window = Window.orderBy(col("total_revenue").desc())
    return agg.withColumn("rank", rank().over(window))


def create_supplier_sales_mart(fact_sales, dim_product, dim_supplier):
    df = (
        fact_sales.join(dim_product, "product_id", "inner")
        .join(dim_supplier, "supplier_id", "left")
    )

    agg = (
        df.groupBy("supplier_id", "supplier_name", "supplier_country")
        .agg(
            _sum("sale_total_price").alias("total_revenue"),
            avg("product_price").alias("avg_product_price"),
            count("*").alias("sale_count"),
        )
    )

    window = Window.orderBy(col("total_revenue").desc())
    return agg.withColumn("rank", rank().over(window))


def create_product_quality_mart(fact_sales, dim_product):
    df = fact_sales.join(dim_product, "product_id", "inner")

    agg = (
        df.groupBy("product_id", "product_name", "product_rating")
        .agg(
            _sum("product_reviews").alias("total_reviews"),
            _sum("sale_total_price").alias("total_revenue"),
            count("*").alias("sale_count"),
        )
    )

    window_rating = Window.orderBy(col("product_rating").desc())
    window_reviews = Window.orderBy(col("total_reviews").desc())

    agg = (
        agg.withColumn("rating_rank", rank().over(window_rating))
        .withColumn("reviews_rank", rank().over(window_reviews))
    )

    return agg


def write_to_clickhouse(df, table_name, mode="overwrite"):
    writer = (
        df.write.format("jdbc")
        .option("url", CLICKHOUSE_URL)
        .option("dbtable", table_name)
        .option("user", "default")
        .option("password", "clickhouse")
        .option("driver", CLICKHOUSE_PROPERTIES["driver"])
        .option("batchsize", "10000")
        .option("numPartitions", "1")
        .mode(mode)
    )
    writer.save()


def main():
    spark = create_spark_session()
    star_schema = read_star_schema(spark)

    fact_sales = star_schema["fact_sales"]
    dim_customer = star_schema["dim_customer"]
    dim_product = star_schema["dim_product"]
    dim_store = star_schema["dim_store"]
    dim_supplier = star_schema["dim_supplier"]
    dim_date = star_schema["dim_date"]

    product_mart = create_product_sales_mart(fact_sales, dim_product)
    write_to_clickhouse(product_mart, "product_sales_mart")

    customer_mart = create_customer_sales_mart(fact_sales, dim_customer)
    write_to_clickhouse(customer_mart, "customer_sales_mart")

    time_mart = create_time_sales_mart(fact_sales, dim_date)
    write_to_clickhouse(time_mart, "time_sales_mart")

    store_mart = create_store_sales_mart(fact_sales, dim_store)
    write_to_clickhouse(store_mart, "store_sales_mart")

    supplier_mart = create_supplier_sales_mart(fact_sales, dim_product, dim_supplier)
    write_to_clickhouse(supplier_mart, "supplier_sales_mart")

    quality_mart = create_product_quality_mart(fact_sales, dim_product)
    write_to_clickhouse(quality_mart, "product_quality_mart")

    spark.stop()


if __name__ == "__main__":
    main()

