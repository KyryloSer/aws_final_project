# glue/jobs/customers/process_customers_bronze_to_silver.py
import sys
from pyspark.sql.functions import col, trim, to_date, lower
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from pyspark.context import SparkContext


def main() -> None:
    args = getResolvedOptions(sys.argv, ["DATALAKE_BUCKET", "BRONZE_PREFIX", "SILVER_PREFIX"])

    bucket = args["DATALAKE_BUCKET"]
    bronze_prefix = args["BRONZE_PREFIX"].strip("/")
    silver_prefix = args["SILVER_PREFIX"].strip("/")

    bronze_path = f"s3://{bucket}/{bronze_prefix}/customers/"
    silver_path = f"s3://{bucket}/{silver_prefix}/customers/"

    sc = SparkContext.getOrCreate()
    glue_context = GlueContext(sc)
    spark = glue_context.spark_session

    bronze_df = spark.read.parquet(bronze_path)

    # Data Cleansing и приведение типов + дедупликация
    silver_df = (
        bronze_df
        .filter(col("ClientId").isNotNull())
        .filter(col("Email").isNotNull())
        .withColumn("client_id", col("ClientId").cast("int"))
        .withColumn("first_name", trim(col("FirstName")))
        .withColumn("last_name", trim(col("LastName")))
        .withColumn("email", lower(trim(col("Email"))))
        .withColumn("registration_date", to_date(col("RegistrationDate"), "yyyy-MM-d"))
        .withColumn("state", trim(col("State")))
        .select("client_id", "first_name", "last_name", "email", "registration_date", "state")
        # Дедупликация: берем последнюю запись по client_id
        .dropDuplicates(["client_id"])
    )

    silver_df.write.mode("overwrite").parquet(silver_path)


if __name__ == "__main__":
    main()
