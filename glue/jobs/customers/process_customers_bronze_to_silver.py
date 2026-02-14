# glue/jobs/customers/process_customers_bronze_to_silver.py
import sys
from pyspark.sql.functions import col, trim, to_date, row_number
from pyspark.sql.window import Window
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from pyspark.context import SparkContext
try:
    from utils.logger import get_logger
    logger = get_logger("process_customers_bronze_to_silver")
except ImportError:
    import logging
    logging.basicConfig(level=logging.INFO)
    logger = logging.getLogger("process_customers_bronze_to_silver")
    logger.warning("utils.logger not found, using default logging")


def main() -> None:
    args = getResolvedOptions(sys.argv, ["DATALAKE_BUCKET", "BRONZE_PREFIX", "SILVER_PREFIX"])

    bucket = args["DATALAKE_BUCKET"]
    bronze_prefix = args["BRONZE_PREFIX"].strip("/")
    silver_prefix = args["SILVER_PREFIX"].strip("/")

    bronze_path = f"s3://{bucket}/{bronze_prefix}/customers/"
    silver_path = f"s3://{bucket}/{silver_prefix}/customers/"

    logger.info("Starting Customers Bronze to Silver job")
    logger.info(f"Bronze path: {bronze_path}")
    logger.info(f"Silver output path: {silver_path}")

    sc = SparkContext.getOrCreate()
    glue_context = GlueContext(sc)
    spark = glue_context.spark_session

    bronze_df = spark.read.parquet(bronze_path)
    
    # 1) Rename columns and trim
    renamed_df = bronze_df.select(
        col("Id").cast("int").alias("client_id"),
        trim(col("FirstName")).alias("first_name"),
        trim(col("LastName")).alias("last_name"),
        trim(col("Email")).alias("email"),
        to_date(trim(col("RegistrationDate")), "yyyy-MM-d").alias("registration_date"),
        trim(col("State")).alias("state")
    )

    # 2) Deduplication: keep latest record by RegistrationDate for each Id
    # Since dumps are incremental, latest file date or just latest registration_date per Id
    window_spec = Window.partitionBy("client_id").orderBy(col("registration_date").desc())
    
    deduplicated_df = (
        renamed_df
        .withColumn("row_num", row_number().over(window_spec))
        .filter(col("row_num") == 1)
        .drop("row_num")
    )

    # 3) Filter NULL IDs if any (sanity check)
    final_df = deduplicated_df.filter(col("client_id").isNotNull())

    logger.info(f"Original count: {bronze_df.count()}, Deduplicated count: {final_df.count()}")

    # 4) Write to silver (NO partitioning as per README)
    (
        final_df
        .write
        .mode("overwrite")
        .parquet(silver_path)
    )

    logger.info("Customers Silver write finished")


if __name__ == "__main__":
    main()
