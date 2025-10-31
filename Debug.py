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

# Check karo months_data_100.parquet mein kya columns hain
input_path = "s3://prototype-aws-elastic-search-123/data/months_data_100.parquet"
df = spark.read.parquet(input_path)

print("=== MONTHS_DATA_100.PARQUET SCHEMA ===")
df.printSchema()

print("=== COLUMNS ===")
print(df.columns)

print("=== SAMPLE DATA ===")
df.show(10)

job.commit()
