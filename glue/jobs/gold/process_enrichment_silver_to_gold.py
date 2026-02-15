import sys
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.context import SparkContext
from pyspark.sql import functions as F

args = getResolvedOptions(
    sys.argv,
    ["JOB_NAME", "DATABASE", "CUSTOMERS_TABLE", "PROFILES_TABLE", "TARGET_S3"]
)

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

db = args["DATABASE"]
cust_tbl = args["CUSTOMERS_TABLE"]
prof_tbl = args["PROFILES_TABLE"]
target_s3 = args["TARGET_S3"].rstrip("/") + "/"

customers = spark.table(f"`{db}`.`{cust_tbl}`")
profiles = spark.table(f"`{db}`.`{prof_tbl}`")

customers2 = customers.select(
    F.col("client_id"),
    F.trim(F.col("first_name")).alias("first_name"),
    F.trim(F.col("last_name")).alias("last_name"),
    F.lower(F.trim(F.col("email"))).alias("email"),
    F.col("registration_date").cast("date").alias("registration_date"),
    F.trim(F.col("state")).alias("customer_state")
)

profiles = spark.table(f"`{db}`.`{prof_tbl}`").select(
    F.lower(F.trim(F.col("email"))).alias("email"),
    F.trim(F.col("full_name")).alias("full_name"),
    F.col("birth_date"),
    F.col("age"),
    F.trim(F.col("state")).alias("profile_state"),
    F.trim(F.col("phone_number")).alias("phone_number")
).dropDuplicates(["email"])

enriched = (
    customers.alias("c")
    .join(profiles.alias("p"), on="email", how="left")
    .select(
        F.col("c.client_id"),
        F.coalesce(F.col("c.first_name"), F.col("p.full_name")).alias("first_name"),
        F.col("c.last_name"),
        F.col("c.email"),
        F.col("c.registration_date"),
        F.coalesce(F.col("p.profile_state"), F.col("c.customer_state")).alias("state"),
        F.col("p.birth_date"),
        F.col("p.age"),
        F.col("p.phone_number"),
        F.current_timestamp().alias("updated_at")
    )
)

enriched.write.mode("overwrite").format("parquet").save(target_s3)
job.commit()
