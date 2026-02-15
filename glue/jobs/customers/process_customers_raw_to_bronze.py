# glue/jobs/customers/process_customers_raw_to_bronze.py
import sys
from pyspark.sql.functions import col, input_file_name, trim
from pyspark.sql.types import StructType, StructField, StringType
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from pyspark.context import SparkContext


def main() -> None:
    args = getResolvedOptions(sys.argv, ["DATALAKE_BUCKET", "RAW_PREFIX", "BRONZE_PREFIX"])

    bucket = args["DATALAKE_BUCKET"]
    raw_prefix = args["RAW_PREFIX"].strip("/")
    bronze_prefix = args["BRONZE_PREFIX"].strip("/")

    # Customers приходят как дампы: последний файл содержит все данные
    raw_path = f"s3://{bucket}/{raw_prefix}/customers/*/*.csv"
    out_path = f"s3://{bucket}/{bronze_prefix}/customers/"

    sc = SparkContext.getOrCreate()
    glue_context = GlueContext(sc)
    spark = glue_context.spark_session

    # Определяем схему (STRING для всех)
    schema = StructType([
        StructField("ClientId", StringType(), True),
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

    bronze_df.write.mode("overwrite").parquet(out_path)


if __name__ == "__main__":
    main()
