import sys

from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job

from pyspark.context import SparkContext
from pyspark.sql import functions as F

from pyspark.sql.functions import (
    col,
    sum,
    count,
    avg
)

args = getResolvedOptions(
    sys.argv,
    ["JOB_NAME", "input_path", "output_path"]
)

input_path = args["input_path"]
output_path = args["output_path"]


# Initialize Glue and Spark
sc = SparkContext.getOrCreate()
glueContext = GlueContext(sc)
spark = glueContext.spark_session

job = Job(glueContext)
job.init(args["JOB_NAME"], args)


# Read extracted Parquet data
df = spark.read.parquet(input_path)

aggregated_df = df.groupBy(
    "Product_Id"
).agg(
    avg("Quantity").alias("total_sales"),
    count("*").alias("number_of_orders")
)

# --------------------------------------------------
# Sort results
# --------------------------------------------------
aggregated_df = aggregated_df.orderBy(
    col("total_sales").desc()
)

# --------------------------------------------------
# Display aggregated results
# --------------------------------------------------
print("Aggregated results:")

aggregated_df.show(
    50,
    truncate=False
)

# --------------------------------------------------
# Write aggregated results to S3
# --------------------------------------------------
print(f"Writing aggregated data to: {output_path}")

aggregated_df.write \
    .mode("overwrite") \
    .option("header", "true") \
    .csv(output_path)

print("Aggregate job completed successfully.")

# --------------------------------------------------
# Commit Glue job
# --------------------------------------------------
job.commit()