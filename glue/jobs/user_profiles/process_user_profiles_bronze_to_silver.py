# glue/jobs/user_profiles/process_user_profiles_bronze_to_silver.py
import sys
from pyspark.sql.functions import col, floor, months_between, current_date, to_date
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from pyspark.context import SparkContext

try:
    from utils.logger import get_logger
    logger = get_logger("process_user_profiles_bronze_to_silver")
except ImportError:
    import logging
    logging.basicConfig(level=logging.INFO)
    logger = logging.getLogger("process_user_profiles_bronze_to_silver")
    logger.warning("utils.logger not found, using default logging")


def main() -> None:
    args = getResolvedOptions(sys.argv, ["DATALAKE_BUCKET", "BRONZE_PREFIX", "SILVER_PREFIX"])

    bucket = args["DATALAKE_BUCKET"]
    bronze_prefix = args["BRONZE_PREFIX"].strip("/")
    silver_prefix = args["SILVER_PREFIX"].strip("/")

    bronze_path = f"s3://{bucket}/{bronze_prefix}/user_profiles/"
    silver_path = f"s3://{bucket}/{silver_prefix}/user_profiles/"

    logger.info("Starting User Profiles Bronze to Silver job")
    logger.info(f"Bronze path: {bronze_path}")
    logger.info(f"Silver output path: {silver_path}")

    sc = SparkContext.getOrCreate()
    glue_context = GlueContext(sc)
    spark = glue_context.spark_session

    bronze_df = spark.read.parquet(bronze_path)

    # Calculate age from birth_date
    silver_df = (
        bronze_df
        .withColumn("reg_date", to_date(col("birth_date"), "yyyy-MM-dd"))
        .withColumn("age", floor(months_between(current_date(), col("reg_date")) / 12).cast("int"))
        .select(
            col("email"),
            col("full_name"),
            col("age"),
            col("state"),
            col("phone_number")
        )
    )

    logger.info(f"Final Silver count: {silver_df.count()}")

    (silver_df.write.mode("overwrite").parquet(silver_path))
    logger.info("User Profiles Silver write finished")


if __name__ == "__main__":
    main()
