# glue/jobs/customers/process_customers_raw_to_bronze.py
import sys
from pyspark.sql.functions import col, input_file_name, trim
from pyspark.sql.types import StructType, StructField, StringType
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from pyspark.context import SparkContext
try:
    from utils.logger import get_logger
    logger = get_logger("process_customers_raw_to_bronze")
except ImportError:
    import logging
    logging.basicConfig(level=logging.INFO)
    logger = logging.getLogger("process_customers_raw_to_bronze")
    logger.warning("utils.logger not found, using default logging")


def main() -> None:
    args = getResolvedOptions(sys.argv, ["DATALAKE_BUCKET", "RAW_PREFIX", "BRONZE_PREFIX"])

    bucket = args["DATALAKE_BUCKET"]
    raw_prefix = args["RAW_PREFIX"].strip("/")
    bronze_prefix = args["BRONZE_PREFIX"].strip("/")

    raw_path = f"s3://{bucket}/{raw_prefix}/customers/*/*.csv"
    out_path = f"s3://{bucket}/{bronze_prefix}/customers/"

    logger.info("Starting Customers Raw to Bronze job")
    logger.info(f"Raw path: {raw_path}")
    logger.info(f"Bronze output path: {out_path}")

    sc = SparkContext.getOrCreate()
    glue_context = GlueContext(sc)
    spark = glue_context.spark_session

    # Schema based on CSV headers: Id, FirstName, LastName, Email, RegistrationDate, State
    schema = StructType([
        StructField("Id", StringType(), True),
        StructField("FirstName", StringType(), True),
        StructField("LastName", StringType(), True),
        StructField("Email", StringType(), True),
        StructField("RegistrationDate", StringType(), True),
        StructField("State", StringType(), True)
    ])

    raw_df = (
        spark.read
        .option("header", "true")
        .option("mode", "PERMISSIVE")
        .schema(schema)
        .csv(raw_path)
    )

    bronze_df = (
        raw_df.select([trim(col(c)).alias(c) for c in raw_df.columns])
              .withColumn("_source_file", input_file_name())
    )

    logger.info(f"Processing {bronze_df.count()} rows")

    (bronze_df.write.mode("overwrite").parquet(out_path))
    logger.info("Customers Bronze write finished")


if __name__ == "__main__":
    main()
