# glue/jobs/sales/process_sales_bronze_to_silver.py
import sys
from pyspark.sql.functions import col, trim, regexp_replace, to_date, round as spark_round
from pyspark.sql.types import DecimalType
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from pyspark.context import SparkContext

from utils.logger import get_logger

logger = get_logger(__name__)


def main() -> None:
    args = getResolvedOptions(sys.argv, ["DATALAKE_BUCKET", "BRONZE_PREFIX", "SILVER_PREFIX"])

    bucket = args["DATALAKE_BUCKET"]
    bronze_prefix = args["BRONZE_PREFIX"].strip("/")
    silver_prefix = args["SILVER_PREFIX"].strip("/")

    bronze_path = f"s3://{bucket}/{bronze_prefix}/sales/"
    silver_path = f"s3://{bucket}/{silver_prefix}/sales/"

    logger.info("Starting job")
    logger.info(f"Bronze path: {bronze_path}")
    logger.info(f"Silver output path: {silver_path}")

    sc = SparkContext.getOrCreate()
    glue_context = GlueContext(sc)
    spark = glue_context.spark_session

    bronze_sales_df = spark.read.parquet(bronze_path)
    bronze_count = bronze_sales_df.count()
    logger.info(f"Bronze row count: {bronze_count}")

    # 1) Rename + trim
    renamed_sales_df = bronze_sales_df.select(
        trim(col("CustomerId")).alias("client_id_raw"),
        trim(col("PurchaseDate")).alias("purchase_date_raw"),
        trim(col("Product")).alias("product_name"),
        trim(col("Price")).alias("price_raw"),
    )

    # 2) Clean price (remove $ , spaces)
    cleaned_price_df = (
        renamed_sales_df
        .withColumn("price_no_symbols", regexp_replace(col("price_raw"), r"[\$,]", ""))
        .withColumn("price_no_symbols", regexp_replace(col("price_no_symbols"), r"\s+", ""))
    )

    # 3) Cast types
    typed_sales_df = (
        cleaned_price_df
        .withColumn("client_id", col("client_id_raw").cast("int"))
        .withColumn("purchase_date", to_date(col("purchase_date_raw"), "yyyy-MM-d"))
        .withColumn("price_double", col("price_no_symbols").cast("double"))
    )

    # 4) Convert price -> DECIMAL(12,2)
    final_sales_df = (
        typed_sales_df
        .withColumn("price", spark_round(col("price_double"), 2).cast(DecimalType(12, 2)))
        .drop("client_id_raw", "purchase_date_raw", "price_raw", "price_no_symbols", "price_double")
    )

    # 5) Basic data quality filter
    filtered_sales_df = final_sales_df.filter(
        col("client_id").isNotNull() &
        col("purchase_date").isNotNull() &
        col("price").isNotNull()
    )

    silver_count = filtered_sales_df.count()
    logger.info(f"Silver row count: {silver_count}")

    # Write silver parquet partitioned by purchase_date
    (
        filtered_sales_df
        .write
        .mode("overwrite")
        .partitionBy("purchase_date")
        .parquet(silver_path)
    )

    logger.info("Silver write finished")


if __name__ == "__main__":
    main()
