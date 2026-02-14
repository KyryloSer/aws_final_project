# glue/jobs/user_profiles/process_user_profiles_raw_to_bronze.py
import sys
from pyspark.sql.functions import input_file_name
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from pyspark.context import SparkContext

try:
    from utils.logger import get_logger
    logger = get_logger("process_user_profiles_raw_to_bronze")
except ImportError:
    import logging
    logging.basicConfig(level=logging.INFO)
    logger = logging.getLogger("process_user_profiles_raw_to_bronze")
    logger.warning("utils.logger not found, using default logging")


def main() -> None:
    args = getResolvedOptions(sys.argv, ["DATALAKE_BUCKET", "RAW_PREFIX", "BRONZE_PREFIX"])

    bucket = args["DATALAKE_BUCKET"]
    raw_prefix = args["RAW_PREFIX"].strip("/")
    bronze_prefix = args["BRONZE_PREFIX"].strip("/")

    raw_path = f"s3://{bucket}/{raw_prefix}/user_profiles/"
    out_path = f"s3://{bucket}/{bronze_prefix}/user_profiles/"

    logger.info("Starting User Profiles Raw to Bronze job")
    logger.info(f"Raw path: {raw_path}")
    logger.info(f"Bronze output path: {out_path}")

    sc = SparkContext.getOrCreate()
    glue_context = GlueContext(sc)
    spark = glue_context.spark_session

    # Read JSONLine
    raw_df = spark.read.json(raw_path)

    # Add source file metadata
    bronze_df = raw_df.withColumn("_source_file", input_file_name())

    logger.info(f"Processing {bronze_df.count()} rows")

    (bronze_df.write.mode("overwrite").parquet(out_path))
    logger.info("User Profiles Bronze write finished")


if __name__ == "__main__":
    main()
