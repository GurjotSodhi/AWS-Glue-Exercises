import sys

from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job

from pyspark.context import SparkContext
from pyspark.sql import functions as F


# Read job parameters from AWS Glue
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


try:
    # Read the raw sales CSV file
    sales_df = (
        spark.read
        .option("header", "true")
        .option("inferSchema", "true")
        .option("mode", "FAILFAST")
        .csv(input_path)
    )

    # Display the source schema and record count
    print("Source schema:")
    sales_df.printSchema()

    record_count = sales_df.count()
    print(f"Records extracted: {record_count}")

    if record_count == 0:
        raise ValueError("The input sales dataset is empty.")

    # Write extracted records to S3 as Parquet
    (
        sales_df.write
        .mode("overwrite")
        .parquet(output_path)
    )

    print(f"Extract job completed: {output_path}")

finally:
    job.commit()