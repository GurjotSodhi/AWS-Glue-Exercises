import sys
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql.functions import col, sum as spark_sum

# --------------------------------------------------
# Get job parameters
# --------------------------------------------------
args = getResolvedOptions(
    sys.argv,
    [
        "JOB_NAME",
        "input_path"
    ]
)

input_path = args["input_path"]

# --------------------------------------------------
# Initialize Glue
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

# --------------------------------------------------
# Basic information
# --------------------------------------------------
print("Validation dataset schema:")
df.printSchema()

record_count = df.count()

print(f"Total records: {record_count}")

# --------------------------------------------------
# Validation 1: Check whether data exists
# --------------------------------------------------
if record_count == 0:
    raise Exception("VALIDATION FAILED: No records found in input dataset.")

print("Validation 1 PASSED: Dataset contains records.")

# --------------------------------------------------
# Validation 2: Check for duplicate records
# --------------------------------------------------
distinct_count = df.dropDuplicates().count()

duplicate_count = record_count - distinct_count

print(f"Duplicate records: {duplicate_count}")

if duplicate_count > 0:
    print("WARNING: Duplicate records found.")
else:
    print("Validation 2 PASSED: No duplicate records found.")

# --------------------------------------------------
# Validation 3: Check for NULL values
# --------------------------------------------------
null_counts = df.select(
    [
        spark_sum(
            col(column).isNull().cast("int")
        ).alias(column)
        for column in df.columns
    ]
)

null_counts.show()

# Check whether any NULL values exist
null_row = null_counts.collect()[0]

total_nulls = sum(
    value if value is not None else 0
    for value in null_row
)

if total_nulls > 0:
    print(
        f"WARNING: Validation found {total_nulls} NULL values."
    )
else:
    print("Validation 3 PASSED: No NULL values found.")

# --------------------------------------------------
# Display sample records
# --------------------------------------------------
print("Sample validated records:")

df.show(10, truncate=False)

# --------------------------------------------------
# Validation completed
# --------------------------------------------------
print("Validation job completed successfully.")

job.commit()