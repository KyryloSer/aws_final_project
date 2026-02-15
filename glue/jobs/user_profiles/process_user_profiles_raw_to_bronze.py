# glue/jobs/user_profiles/process_user_profiles_raw_to_bronze.py
import sys
from pyspark.sql.functions import col, input_file_name, trim
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from pyspark.context import SparkContext


def main() -> None:
    args = getResolvedOptions(sys.argv, ["DATALAKE_BUCKET", "RAW_PREFIX", "BRONZE_PREFIX"])

    bucket = args["DATALAKE_BUCKET"]
    raw_prefix = args["RAW_PREFIX"].strip("/")
    bronze_prefix = args["BRONZE_PREFIX"].strip("/")

    raw_path = f"s3://{bucket}/{raw_prefix}/user_profiles/*.json"
    out_path = f"s3://{bucket}/{bronze_prefix}/user_profiles/"

    sc = SparkContext.getOrCreate()
    glue_context = GlueContext(sc)
    spark = glue_context.spark_session

    # Читаем JSON (multiLine=false для JSONLine формата)
    raw_df = spark.read.option("multiLine", "false").json(raw_path)

    bronze_df = raw_df.withColumn("_source_file", input_file_name())

    bronze_df.write.mode("overwrite").parquet(out_path)


if __name__ == "__main__":
    main()
