# glue/jobs/user_profiles/process_user_profiles_bronze_to_silver.py
import sys
from pyspark.sql.functions import col, trim, lower, to_date, floor, months_between, current_date
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from pyspark.context import SparkContext


def main() -> None:
    args = getResolvedOptions(sys.argv, ["DATALAKE_BUCKET", "BRONZE_PREFIX", "SILVER_PREFIX"])

    bucket = args["DATALAKE_BUCKET"]
    bronze_prefix = args["BRONZE_PREFIX"].strip("/")
    silver_prefix = args["SILVER_PREFIX"].strip("/")

    bronze_path = f"s3://{bucket}/{bronze_prefix}/user_profiles/"
    silver_path = f"s3://{bucket}/{silver_prefix}/user_profiles/"

    sc = SparkContext.getOrCreate()
    glue_context = GlueContext(sc)
    spark = glue_context.spark_session

    bronze_df = spark.read.parquet(bronze_path)

    # Data Cleansing
    silver_df = (
        bronze_df
        .filter(col("email").isNotNull())
        .withColumn("birth_date", to_date(col("birth_date"), "yyyy-MM-dd"))
        .withColumn("age", floor(months_between(current_date(), col("birth_date")) / 12).cast("int"))
        .withColumn("email", lower(trim(col("email"))))
        .withColumn("full_name", trim(col("full_name")))
        .withColumn("state", trim(col("state")))
        .withColumn("phone_number", trim(col("phone_number")))
        .select("email", "full_name", "birth_date", "age", "state", "phone_number")
        .dropDuplicates(["email"])
    )


    silver_df.write.mode("overwrite").parquet(silver_path)


if __name__ == "__main__":
    main()
