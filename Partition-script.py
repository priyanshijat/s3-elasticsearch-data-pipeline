import sys
from awsglue.context import GlueContext
from awsglue.utils import getResolvedOptions
from awsglue.job import Job
from pyspark.context import SparkContext

args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# October ka data read karo
input_path = "s3://prototype-aws-elastic-search-123/data/data_october_10000000.parquet"
df = spark.read.parquet(input_path)

print("=== OCTOBER DATA INFO ===")
print(f"Total records: {df.count()}")

# Sirf October filter karo (agar needed ho)
october_df = df.filter(df.month == "October")
print(f"October records: {october_df.count()}")

# Verify columns
print("=== COLUMNS IN OCTOBER DATA ===")
print(october_df.columns)

print("=== SAMPLE DATA ===")
october_df.select("id", "month", "temperature", "humidity").show(10)

# Partitioned data mein ADD karo
output_path = "s3://prototype-aws-elastic-search-123/new-partition/"
october_df.write \
    .mode("append") \
    .partitionBy("month") \
    .format("parquet") \
    .save(output_path)

print(f"✅ October data ADDED to: {output_path}")

# Verify all partitions
print("=== ALL PARTITIONS NOW ===")
partition_df = spark.read.parquet(output_path)
distinct_months = [row['month'] for row in partition_df.select("month").distinct().collect()]
print("Months in partition:", sorted(distinct_months))

print("=== RECORD COUNT BY MONTH ===")
partition_df.groupBy("month").count().show()

job.commit()
