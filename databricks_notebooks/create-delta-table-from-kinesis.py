# Databricks notebook source
# MAGIC %md
# MAGIC Once the streaming data has been cleaned, you should save each stream in a Delta Table. You should save the following tables: 
# MAGIC   - {your_UserId}_pin_table
# MAGIC   - {your_UserId}_geo_table
# MAGIC   - {your_UserId}_user_table.

# COMMAND ----------

# pyspark functions
from pyspark.sql.functions import *
import pyspark.sql.functions as F
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, TimestampType, DecimalType
# URL processing
import urllib

# COMMAND ----------

# Define the path to the Delta table
delta_table_path = "dbfs:/user/hive/warehouse/authentication_credentials"

# Read the Delta table to a Spark DataFrame
aws_keys_df = spark.read.format("delta").load(delta_table_path)

# COMMAND ----------

# Get the AWS access key and secret key from the spark dataframe
ACCESS_KEY = aws_keys_df.select('Access key ID').collect()[0]['Access key ID']
SECRET_KEY = aws_keys_df.select('Secret access key').collect()[0]['Secret access key']
# Encode the secrete key
ENCODED_SECRET_KEY = urllib.parse.quote(string=SECRET_KEY, safe="")

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS 0affcd87e38f_pin_table (
# MAGIC   ind INT,
# MAGIC   unique_id STRING,
# MAGIC   title STRING,
# MAGIC   description STRING,
# MAGIC   follower_count INT,
# MAGIC   poster_name STRING,
# MAGIC   tag_list STRING,
# MAGIC   is_image_or_video STRING,
# MAGIC   image_src STRING,
# MAGIC   save_location STRING,
# MAGIC   category STRING,
# MAGIC   downloaded INT
# MAGIC )
# MAGIC USING delta
# MAGIC LOCATION '/mnt/delta/0affcd87e38f_pin_table'

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS 0affcd87e38f_geo_table (
# MAGIC   ind INT,
# MAGIC   timestamp TIMESTAMP,
# MAGIC   latitude DECIMAL,
# MAGIC   longitude DECIMAL,
# MAGIC   country STRING
# MAGIC )
# MAGIC USING delta
# MAGIC LOCATION '/mnt/delta/0affcd87e38f_geo_table'

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS 0affcd87e38f_user_table (
# MAGIC   ind INT,
# MAGIC   first_name STRING,
# MAGIC   last_name STRING,
# MAGIC   age INT,
# MAGIC   date_joined TIMESTAMP
# MAGIC )
# MAGIC USING delta
# MAGIC LOCATION '/mnt/delta/0affcd87e38f_user_table'

# COMMAND ----------

# MAGIC %md
# MAGIC STREAMING PIN

# COMMAND ----------

pin_schema = StructType([
    StructField("ind", IntegerType(), True),
    StructField("unique_id", StringType(), True),
    StructField("title", StringType(), True),
    StructField("description", StringType(), True),
    StructField("follower_count", IntegerType(), True),
    StructField("poster_name", StringType(), True),
    StructField("tag_list", StringType(), True),
    StructField("is_image_or_video", StringType(), True),
    StructField("image_src", StringType(), True),
    StructField("save_location", StringType(), True),
    StructField("category", StringType(), True),
    StructField("downloaded", IntegerType(), True),
])

df_pin_from_kinesis = spark \
    .readStream \
    .format('kinesis') \
    .option('streamName', 'streaming-0affcd87e38f-pin') \
    .option('initialPosition','latest') \
    .option('region', 'us-east-1') \
    .option('awsAccessKey', ACCESS_KEY) \
    .option('awsSecretKey', SECRET_KEY) \
    .load() \
    
df_pin_from_kinesis = df_pin_from_kinesis \
    .selectExpr("CAST(data as STRING) jsonData") \
    .select(from_json("jsonData", schema=pin_schema).alias("columns")) \
    .select("columns.*")

#display(df_pin_from_kinesis)

# COMMAND ----------

dbutils.fs.rm("/tmp/kinesis/_checkpoints/", True)

df_pin_from_kinesis.writeStream \
  .format("delta") \
  .outputMode("append") \
  .option("checkpointLocation", "/tmp/kinesis/_checkpoints/") \
  .option("mergeSchema", "true") \
  .table("0affcd87e38f_pin_table")


# COMMAND ----------

# MAGIC %md
# MAGIC STREAMING GEO

# COMMAND ----------

geo_schema = StructType([
    StructField("ind", IntegerType(), True),
    StructField("timestamp", TimestampType(), True),
    StructField("latitude", DecimalType(), True),
    StructField("longitude", DecimalType(), True),
    StructField("country", StringType(), True)
])

df_geo_from_kinesis = spark \
    .readStream \
    .format('kinesis') \
    .option('streamName', 'streaming-0affcd87e38f-geo') \
    .option('initialPosition','latest') \
    .option('region', 'us-east-1') \
    .option('awsAccessKey', ACCESS_KEY) \
    .option('awsSecretKey', SECRET_KEY) \
    .load() \
    
df_geo_from_kinesis = df_geo_from_kinesis \
    .selectExpr("CAST(data as STRING) jsonData") \
    .select(from_json("jsonData", schema=geo_schema).alias("columns")) \
    .select("columns.*")

display(df_geo_from_kinesis)

# COMMAND ----------

dbutils.fs.rm("/tmp/kinesis/_checkpoints/", True)

df_pin_from_kinesis.writeStream \
  .format("delta") \
  .outputMode("append") \
  .option("checkpointLocation", "/tmp/kinesis/_checkpoints/") \
  .option("mergeSchema", "true") \
  .table("0affcd87e38f_geo_table")

# COMMAND ----------

# MAGIC %md
# MAGIC STREAMING USER
# MAGIC

# COMMAND ----------

user_schema = StructType([
    StructField("ind", IntegerType(), True),
    StructField("first_name", StringType(), True),
    StructField("last_name", StringType(), True),
    StructField("date_joined", TimestampType(), True),
])

df_user_from_kinesis = spark \
    .readStream \
    .format('kinesis') \
    .option('streamName', 'streaming-0affcd87e38f-user') \
    .option('initialPosition','latest') \
    .option('region', 'us-east-1') \
    .option('awsAccessKey', ACCESS_KEY) \
    .option('awsSecretKey', SECRET_KEY) \
    .load() \
    
df_user_from_kinesis = df_user_from_kinesis \
    .selectExpr("CAST(data as STRING) jsonData") \
    .select(from_json("jsonData", schema=user_schema).alias("columns")) \
    .select("columns.*")

# COMMAND ----------

dbutils.fs.rm("/tmp/kinesis/_checkpoints/", True)

df_pin_from_kinesis.writeStream \
  .format("delta") \
  .outputMode("append") \
  .option("checkpointLocation", "/tmp/kinesis/_checkpoints/") \
  .option("mergeSchema", "true") \
  .table("0affcd87e38f_geo_table")
