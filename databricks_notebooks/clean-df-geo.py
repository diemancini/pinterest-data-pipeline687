# Databricks notebook source
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

# MAGIC %md
# MAGIC To clean the df_geo DataFrame you should perform the following transformations:
# MAGIC
# MAGIC - Create a new column coordinates that contains an array based on the latitude and longitude columns
# MAGIC -  Drop the latitude and longitude columns from the DataFrame
# MAGIC - Convert the timestamp column from a string to a timestamp data type
# MAGIC - Reorder the DataFrame columns to have the following column order:
# MAGIC     - ind
# MAGIC     - country
# MAGIC     - coordinates
# MAGIC     - timestamp

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.functions import unix_timestamp, array
from pyspark.sql.types import ( 
    StringType, IntegerType
)

class SparkGeoCleaner:

    COUNTRY_INDEX = "country"
    IND_INDEX = "ind"
    LATITUDE_INDEX = "latitude"
    LONGITUDE_INDEX = "longitude"
    TIMESTAMP_INDEX = "timestamp"
    COORDINATES_INDEX = "coordinates"
    
    def reorder_df_geo_columns(self, df):
        """
        Reorder the columns of the GEO dataframe.
        Parameters:
            - df: DataFrame
        """
        df = df.select(
            self.IND_INDEX,
            self.COUNTRY_INDEX,
            self.COORDINATES_INDEX,
            self.TIMESTAMP_INDEX
        )

        return df

    def clean_empty_df_geo(self, df: DataFrame):
        """
        Clean the empty columns of the GEO dataframe.
        Parameters:
            - df: DataFrame
        """
        df = df\
            .withColumn(self.COORDINATES_INDEX, array(df[self.LATITUDE_INDEX], df[self.LONGITUDE_INDEX]))\
                .drop(self.LATITUDE_INDEX, self.LONGITUDE_INDEX)
        df = df\
            .withColumn(self.TIMESTAMP_INDEX, col(self.TIMESTAMP_INDEX).cast('timestamp'))

        df = self.reorder_df_geo_columns(df)

        return df

geo_cleaner = SparkGeoCleaner()
df = get_dataframe_from_drive("0affcd87e38f.geo")
df_geo_cleaned = geo_cleaner.clean_empty_df_geo(df)
df_geo_cleaned.printSchema()
display(df_geo_cleaned)
dbutils.notebook.exit(df_geo_cleaned)
