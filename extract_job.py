import sys
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

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
# Initialize Glue
# --------------------------------------------------
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session

job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# --------------------------------------------------
# Read raw sales data
# --------------------------------------------------
print(f"Reading input data from: {input_path}")

df = spark.read \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .csv(input_path)

print("Input schema:")
df.printSchema()

print(f"Input record count: {df.count()}")

# --------------------------------------------------
# Basic extraction / cleanup
# --------------------------------------------------

# Remove completely empty rows
df = df.dropna(how="all")

# Remove duplicate records
df = df.dropDuplicates()

print(f"Record count after basic cleanup: {df.count()}")

# --------------------------------------------------
# Write extracted data
# --------------------------------------------------
print(f"Writing extracted data to: {output_path}")

df.write \
    .mode("overwrite") \
    .option("header", "true") \
    .csv(output_path)

print("Extract job completed successfully.")

# --------------------------------------------------
# Commit Glue job
# --------------------------------------------------
job.commit()