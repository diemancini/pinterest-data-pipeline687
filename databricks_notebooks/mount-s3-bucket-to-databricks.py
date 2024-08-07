# Databricks notebook source
# pyspark functions
from pyspark.sql.functions import *
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

# AWS S3 bucket name
AWS_S3_BUCKET = "user-0affcd87e38f-bucket"
# Mount name for the bucket
MOUNT_NAME = "/mnt/s3_0affcd87e38f-bucket"
# Source url
SOURCE_URL = "s3n://{0}:{1}@{2}".format(ACCESS_KEY, ENCODED_SECRET_KEY, AWS_S3_BUCKET)
# Mount the drive
dbutils.fs.mount(SOURCE_URL, MOUNT_NAME)

# COMMAND ----------

display(dbutils.fs.ls("/mnt/s3_0affcd87e38f-bucket/topics/0affcd87e38f.pin/partition=0"))

# COMMAND ----------


## Disable format checks during the reading of Delta tables
spark.sql("SET spark.databricks.delta.formatCheck.enabled=false")

# COMMAND ----------

def get_dataframe_from_drive(topic):
    # File location and type
    # Asterisk(*) indicates reading all the content of the specified file that have .json extension
    file_location = f"/mnt/s3_0affcd87e38f-bucket/topics/{topic}/partition=0/*.json" 
    file_type = "json"
    # Ask Spark to infer the schema
    infer_schema = "true"
    # Read in JSONs from mounted S3 bucket
    df = spark.read.format(file_type) \
    .option("inferSchema", infer_schema) \
    .load(file_location)

    return df


# COMMAND ----------



# COMMAND ----------

#import sys
#sys.path.append("/Workspace/Users/diemancini@gmail.com")
#sys.path.remove("/Workspace/Shared")
# sys.path.append("/Workspace/scripts")
# sys.path
#from utils import get_dataframe_from_drive
#%run "./scripts/utils.py"
df_pin = get_dataframe_from_drive("0affcd87e38f.pin")
df_geo = get_dataframe_from_drive("0affcd87e38f.geo")
df_user = get_dataframe_from_drive("0affcd87e38f.user")                                  
# Display Spark dataframe to check its content
display(df_pin)
display(df_geo)
display(df_user)