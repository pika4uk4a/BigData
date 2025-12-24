from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    to_date,
    year,
    month,
    quarter,
    dayofweek,
    expr,
    row_number,
)
from pyspark.sql.window import Window

POSTGRES_URL = "jdbc:postgresql://postgres:5432/spark_db"
POSTGRES_PROPERTIES = {
    "user": "postgres",
    "password": "postgres",
    "driver": "org.postgresql.Driver",
}


def create_spark_session():
    return SparkSession.builder.appName("ETL to Star Schema").getOrCreate()


def read_mock_data(spark):
    return (
        spark.read.format("jdbc")
        .option("url", POSTGRES_URL)
        .option("dbtable", "mock_data")
        .option("user", POSTGRES_PROPERTIES["user"])
        .option("password", POSTGRES_PROPERTIES["password"])
        .option("driver", POSTGRES_PROPERTIES["driver"])
        .load()
    )


def _with_surrogate_id(df, order_cols, id_col):
    window = Window.orderBy(*order_cols)
    return df.withColumn(id_col, row_number().over(window))


def create_dim_supplier(df):
    dim = (
        df.select(
            "supplier_name",
            "supplier_contact",
            "supplier_email",
            "supplier_phone",
            "supplier_address",
            "supplier_city",
            "supplier_country",
        )
        .dropDuplicates(["supplier_name"])
    )
    return _with_surrogate_id(dim, ["supplier_name"], "supplier_id")


def create_dim_customer(df):
    dim = (
        df.select(
            "customer_first_name",
            "customer_last_name",
            "customer_age",
            "customer_email",
            "customer_country",
            "customer_postal_code",
            "customer_pet_type",
            "customer_pet_name",
            "customer_pet_breed",
        )
        .dropDuplicates(["customer_email"])
    )
    return _with_surrogate_id(dim, ["customer_email"], "customer_id")


def create_dim_seller(df):
    dim = (
        df.select(
            "seller_first_name",
            "seller_last_name",
            "seller_email",
            "seller_country",
            "seller_postal_code",
        )
        .dropDuplicates(["seller_email"])
    )
    return _with_surrogate_id(dim, ["seller_email"], "seller_id")


def create_dim_store(df):
    dim = (
        df.select(
            "store_name",
            "store_location",
            "store_city",
            "store_state",
            "store_country",
            "store_phone",
            "store_email",
        )
        .dropDuplicates(["store_name"])
    )
    return _with_surrogate_id(dim, ["store_name"], "store_id")


def create_dim_product(df, dim_supplier):
    dim = (
        df.select(
            "product_name",
            "product_category",
            "product_price",
            "product_quantity",
            "pet_category",
            "product_weight",
            "product_color",
            "product_size",
            "product_brand",
            "product_material",
            "product_description",
            "product_rating",
            "product_reviews",
            "product_release_date",
            "product_expiry_date",
            "supplier_name",
        )
        .dropDuplicates(["product_name"])
    )

    dim = (
        dim.alias("p")
        .join(
            dim_supplier.select("supplier_id", "supplier_name").alias("s"),
            col("p.supplier_name") == col("s.supplier_name"),
            "left",
        )
        .select(
            "p.product_name",
            "product_category",
            "product_price",
            "product_quantity",
            "pet_category",
            "product_weight",
            "product_color",
            "product_size",
            "product_brand",
            "product_material",
            "product_description",
            "product_rating",
            "product_reviews",
            "product_release_date",
            "product_expiry_date",
            "s.supplier_id",
        )
    )

    return _with_surrogate_id(dim, ["product_name"], "product_id")


def create_dim_date(df):
    dt = (
        df.select(to_date(col("sale_date"), "M/d/yyyy").alias("sale_date"))
        .filter(col("sale_date").isNotNull())
        .dropDuplicates(["sale_date"])
    )

    dt = (
        dt.withColumn("year", year("sale_date"))
        .withColumn("month", month("sale_date"))
        .withColumn("quarter", quarter("sale_date"))
        .withColumn("day_of_week", dayofweek("sale_date"))
        .withColumn("is_weekend", expr("day_of_week in (1,7)"))
    )

    return _with_surrogate_id(dt, ["sale_date"], "date_id")


def create_fact_sales(df, dim_customer, dim_seller, dim_product, dim_store, dim_date):
    df_date = dim_date.select("date_id", col("sale_date").alias("dt"))

    fact = (
        df.join(dim_customer, "customer_email", "inner")
        .join(dim_seller, "seller_email", "inner")
        .join(dim_product, "product_name", "inner")
        .join(dim_store, "store_name", "inner")
        .join(df_date, to_date(col("sale_date"), "M/d/yyyy") == col("dt"), "inner")
        .select(
            col("dt").alias("sale_date"),
            "date_id",
            "customer_id",
            "seller_id",
            "product_id",
            "store_id",
            col("sale_quantity").cast("int"),
            col("sale_total_price").cast("decimal(10,2)"),
        )
    )

    return fact


def write_to_postgres(df, table_name, mode="overwrite"):
    (
        df.write.format("jdbc")
        .option("url", POSTGRES_URL)
        .option("dbtable", table_name)
        .option("user", POSTGRES_PROPERTIES["user"])
        .option("password", POSTGRES_PROPERTIES["password"])
        .option("driver", POSTGRES_PROPERTIES["driver"])
        .mode(mode)
        .save()
    )


def main():
    spark = create_spark_session()
    mock_data = read_mock_data(spark)

    dim_supplier = create_dim_supplier(mock_data)
    dim_customer = create_dim_customer(mock_data)
    dim_seller = create_dim_seller(mock_data)
    dim_store = create_dim_store(mock_data)
    dim_product = create_dim_product(mock_data, dim_supplier)
    dim_date = create_dim_date(mock_data)

    fact_sales = create_fact_sales(
        mock_data,
        dim_customer,
        dim_seller,
        dim_product,
        dim_store,
        dim_date,
    )

    write_to_postgres(dim_supplier, "dim_supplier")
    write_to_postgres(dim_customer, "dim_customer")
    write_to_postgres(dim_seller, "dim_seller")
    write_to_postgres(dim_store, "dim_store")
    write_to_postgres(dim_product, "dim_product")
    write_to_postgres(dim_date, "dim_date")
    write_to_postgres(fact_sales, "fact_sales")

    spark.stop()


if __name__ == "__main__":
    main()
