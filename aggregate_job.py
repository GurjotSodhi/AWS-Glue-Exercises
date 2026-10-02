```python
import sys

from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

from pyspark.sql.functions import (
    col,
    sum as spark_sum,
    count,
    avg
)

# --------------------------------------------------
# Get job parameters
# --------------------------------------------------
args = getResolvedOptions(
    sys.argv,
    [
        "JOB_NAME",
        "input_path",
        "output_path"
    ]
)

input_path = args["input_path"]
output_path = args["output_path"]

# --------------------------------------------------
# Initialize Spark / Glue
# --------------------------------------------------
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session

job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# --------------------------------------------------
# Read extracted data
# --------------------------------------------------
print(f"Reading extracted data from: {input_path}")

df = spark.read \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .csv(input_path)

print("Input schema:")
df.printSchema()

print(f"Input record count: {df.count()}")

# --------------------------------------------------
# Display sample input
# --------------------------------------------------
df.show(10, truncate=False)

# --------------------------------------------------
# Aggregate sales data
#
# Expected columns for this example:
#   product / category
#   quantity
#   sales_amount
#
# Change these column names if your
# sales_data.csv uses different names.
# --------------------------------------------------

aggregated_df = df.groupBy(
    "product"
).agg(
    spark_sum("quantity").alias("total_quantity"),
    spark_sum("sales_amount").alias("total_sales"),
    avg("sales_amount").alias("average_sales"),
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
```
