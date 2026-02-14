# glue/jobs/sales/process_sales_raw_to_bronze.py
import sys
from pyspark.sql.functions import col, input_file_name, trim
from pyspark.sql.types import StructType, StructField, StringType
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from pyspark.context import SparkContext

from utils.logger import get_logger
    
    
logger = get_logger(__name__)


def main() -> None:
    args = getResolvedOptions(sys.argv, ["DATALAKE_BUCKET", "RAW_PREFIX", "BRONZE_PREFIX"])

    bucket = args["DATALAKE_BUCKET"]
    raw_prefix = args["RAW_PREFIX"].strip("/")
    bronze_prefix = args["BRONZE_PREFIX"].strip("/")

    # ВАЖНО: Добавляем /* в конце для рекурсивного чтения всех папок с датами
    raw_path = f"s3://{bucket}/{raw_prefix}/sales/*/*.csv"
    out_path = f"s3://{bucket}/{bronze_prefix}/sales/"

    logger.info("Starting job")
    logger.info(f"Raw path: {raw_path}")
    logger.info(f"Bronze output path: {out_path}")

    sc = SparkContext.getOrCreate()
    glue_context = GlueContext(sc)
    spark = glue_context.spark_session

    # Определяем схему явно, как требует Bronze слой (STRING для всех)
    schema = StructType([
        StructField("CustomerId", StringType(), True),
        StructField("PurchaseDate", StringType(), True),
        StructField("Product", StringType(), True),
        StructField("Price", StringType(), True)
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

    # Выполянем count только если нужно логировать, это вызывает вычисления
    bronze_count = bronze_df.count()
    logger.info(f"Bronze row count: {bronze_count}")

    (bronze_df.write.mode("overwrite").parquet(out_path))
    logger.info("Bronze write finished")


if __name__ == "__main__":
    main()
