# Databricks notebook source
# pyspark functions
from pyspark.sql.functions import *
# URL processing
import urllib

# COMMAND ----------

# MAGIC %md
# MAGIC Step 1:\
# MAGIC Create a new Notebook in Databricks and read in your credentials from the Delta table, located at dbfs:/user/hive/warehouse/authentication_credentials, to retrieve the Access Key and Secret Access Key. Follow the same process for this, as you have followed for your batch data.

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

## Disable format checks during the reading of Delta tables
spark.sql("SET spark.databricks.delta.formatCheck.enabled=false")

# COMMAND ----------

# MAGIC %md
# MAGIC Step 2:
# MAGIC
# MAGIC Run your preferred method to ingest data into Kinesis Data Streams. In the Kinesis console, check your data streams are receiving the data.
# MAGIC

# COMMAND ----------

# MAGIC %run "./clean-df-pin"

# COMMAND ----------

# MAGIC %run "./clean-df-geo"

# COMMAND ----------

# MAGIC %run "./clean-df-user"

# COMMAND ----------

from typing import Dict
import requests

def serialize_datetime(data: Dict) -> Dict:
        """
        Convert datetime to string in YYYY-mm-DD HH:MM:SS format.

        Parameters:
            - data: Dict
        """
        keys = list(data.keys())
        for key in keys:
            if isinstance(data[key], datetime):
                data[key] = data[key].strftime("%Y-%m-%d %H:%M:%S")
        return data

def serialize_datetime_put_records(data: Dict) -> Dict:
    """
    Convert datetime to string in YYYY-mm-DD HH:MM:SS format.

    Parameters:
        - data: Dict
    """
    if isinstance(data, list):
        keys = list(data[0]["data"].keys())
        logger.info(keys)
        for i in range(len(data)):
            for key in keys:
                if isinstance(data[i]["data"][key], datetime):
                    #print(data[i]["data"][key])
                    data[i]["data"][key] = data[i]["data"][key].strftime("%Y-%m-%d %H:%M:%S")
    else:
        data = serialize_datetime(data)
    return data


def http_stream(data: Dict={}, stream_name: str="", record: str = "record", method="POST") -> None:
        """
        Read/Send data to Kinesis AWS.
        Where data contains the info that it should sent to the server.

        Parameters:
            - data: Dict,
            - stream_name: string -> Name of Kinesis stream.
            - record: str -> Url path for creating or updating data.
            - method: str
        """
        INVOKE_URL_STREAM = "https://74y1om8mn3.execute-api.us-east-1.amazonaws.com/kinesis"
        invoke_url = f"{INVOKE_URL_STREAM}/streams/{stream_name}"
        data = self.serialize_datetime_put_records(data)
        payload = {}
        if method.upper() == "PUT":
            invoke_url += f"/{record}"
        if method.upper() == "PUT" and record == "records":
            payload = json.dumps({
                "StreamName": stream_name,
                "records": data
            })
        elif method.upper() == "PUT" or method.upper() == "POST":
            data = serialize_datetime(data)
            payload = json.dumps({
                "StreamName": stream_name,
                "Data": data,
                "PartitionKey": "partition-1"
            })
        #print(payload)

        headers =  {'Content-Type': 'application/json'}
        response = requests.request(method, invoke_url, headers=headers, data=payload)
        print(response)
        print(response.content)


# COMMAND ----------

import json
df_json_pin = df_pin_cleaned.toJSON().map(json.loads).collect()
df_json_geo = df_geo_cleaned.toJSON().map(json.loads).collect()
df_json_user = df_user_cleaned.toJSON().map(json.loads).collect()
#print(df_json_pin)
data_pin = [{"data": row, "partition-key": f"partition-{i}"} for i, row in enumerate(df_json_pin)]
data_geo = [{"data": row, "partition-key": f"partition-{i}"} for i, row in enumerate(df_json_geo)]
data_user = [{"data": row, "partition-key": f"partition-{i}"} for i, row in enumerate(df_json_user)]
# print(data_pin)
http_stream(data=data_pin, stream_name="streaming-0affcd87e38f-pin", record="records", method="PUT")
http_stream(data=data_geo, stream_name="streaming-0affcd87e38f-geo", record="records", method="PUT")
http_stream(data=data_user, stream_name="streaming-0affcd87e38f-user", record="records", method="PUT")


# COMMAND ----------

# MAGIC %md
# MAGIC Step 3:
# MAGIC
# MAGIC Read the data from the three streams you have created in your Databricks Notebook.

# COMMAND ----------

df_pin_from_kinesis = spark \
    .readStream \
    .format('kinesis') \
    .option('streamName', 'streaming-0affcd87e38f-pin') \
    .option('initialPosition','earliest') \
    .option('region', 'us-east-1') \
    .option('awsAccessKey', ACCESS_KEY) \
    .option('awsSecretKey', SECRET_KEY) \
    .load()

display(df_pin_from_kinesis)

# COMMAND ----------

df_geo_from_kinesis  = spark \
    .readStream \
    .format('kinesis') \
    .option('streamName', 'streaming-0affcd87e38f-geo') \
    .option('initialPosition','earliest') \
    .option('region', 'us-east-1') \
    .option('awsAccessKey', ACCESS_KEY) \
    .option('awsSecretKey', SECRET_KEY) \
    .load()

display(df_geo_from_kinesis)

# COMMAND ----------

df_user_from_kinesis  = spark \
    .readStream \
    .format('kinesis') \
    .option('streamName', 'streaming-0affcd87e38f-user') \
    .option('initialPosition','earliest') \
    .option('region', 'us-east-1') \
    .option('awsAccessKey', ACCESS_KEY) \
    .option('awsSecretKey', SECRET_KEY) \
    .load()

display(df_user_from_kinesis)
