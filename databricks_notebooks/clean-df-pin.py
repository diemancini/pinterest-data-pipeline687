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
# MAGIC To clean the df_pin DataFrame you should perform the following transformations:
# MAGIC
# MAGIC - Replace empty entries and entries with no relevant data in each column with Nones.
# MAGIC - Perform the necessary transformations on the follower_count to ensure every entry is a number. Make sure the data type of this column is an int.
# MAGIC - Ensure that each column containing numeric data has a numeric data type.
# MAGIC - Clean the data in the save_location column to include only the save location path.
# MAGIC - Rename the index column to ind.
# MAGIC - Reorder the DataFrame columns to have the following column order:<br>
# MAGIC     - ind
# MAGIC     - unique_id
# MAGIC     - title
# MAGIC     - description
# MAGIC     - follower_count
# MAGIC     - poster_name
# MAGIC     - tag_list
# MAGIC     - is_image_or_video
# MAGIC     - image_src
# MAGIC     - save_location
# MAGIC     - category

# COMMAND ----------

import re

from pyspark.sql.functions import *
from pyspark.sql.functions import regexp_replace
from pyspark.sql.types import ( 
    StringType, IntegerType
)

class SparkPinCleaner:

    IND_INDEX = "ind"
    INDEX_INDEX = "index"
    UNIQUE_INDEX = "unique_id"
    TITLE_INDEX = "title"
    DESCRIPTION_INDEX = "description"
    FOLLOWER_COUNT_INDEX = "follower_count"
    POSTER_NAME_INDEX = "poster_name"
    TAG_LIST_INDEX = "tag_list"
    IS_IMAGE_OR_VIDEO_INDEX = "is_image_or_video"
    IMAGE_SRC_INDEX = "image_src"
    SAVE_LOCATION_INDEX = "save_location"
    CATEGORY_INDEX = "category"
    DOWNLOADED_INDEX = "downloaded"

    def set_pin_columns_types(self, df: DataFrame):
        """
        Set the data types of the columns of the dataframe.
        Parameters:
            - df: DataFrame
        """

        df = df\
            .withColumn(self.CATEGORY_INDEX, df[self.CATEGORY_INDEX].cast(StringType()))\
            .withColumn(self.DESCRIPTION_INDEX, df[self.DESCRIPTION_INDEX].cast(StringType()))\
            .withColumn(self.DOWNLOADED_INDEX, df[self.DOWNLOADED_INDEX].cast(IntegerType()))\
            .withColumn(self.FOLLOWER_COUNT_INDEX, df[self.FOLLOWER_COUNT_INDEX].cast(IntegerType()))\
            .withColumn(self.IMAGE_SRC_INDEX, df[self.IMAGE_SRC_INDEX].cast(StringType()))\
            .withColumn(self.INDEX_INDEX, df[self.INDEX_INDEX].cast(IntegerType()))\
            .withColumn(self.IS_IMAGE_OR_VIDEO_INDEX, df[self.IS_IMAGE_OR_VIDEO_INDEX].cast(StringType()))\
            .withColumn(self.POSTER_NAME_INDEX, df[self.POSTER_NAME_INDEX].cast(StringType()))\
            .withColumn(self.SAVE_LOCATION_INDEX, df[self.SAVE_LOCATION_INDEX].cast(StringType()))\
            .withColumn(self.TAG_LIST_INDEX, df[self.TAG_LIST_INDEX].cast(StringType()))\
            .withColumn(self.TITLE_INDEX, df[self.TITLE_INDEX].cast(StringType()))\
            .withColumn(self.UNIQUE_INDEX, df[self.UNIQUE_INDEX].cast(StringType()))

        df.printSchema()

        return df

    def reorder_df_pin_columns(self, df):
        """
        Reorder the columns of the PIN dataframe.
        Parameters:
            - df: DataFrame
        """
        df = df.select(
            col(self.INDEX_INDEX).alias(self.IND_INDEX), 
            self.UNIQUE_INDEX,
            self.TITLE_INDEX, 
            self.DESCRIPTION_INDEX,
            self.FOLLOWER_COUNT_INDEX,
            self.POSTER_NAME_INDEX, 
            self.TAG_LIST_INDEX, 
            self.IS_IMAGE_OR_VIDEO_INDEX, 
            self.IMAGE_SRC_INDEX, 
            self.SAVE_LOCATION_INDEX, 
            self.CATEGORY_INDEX, 
            self.DOWNLOADED_INDEX
        )

        return df

    def clean_empty_df_pin(self, df: DataFrame):
        """
        Clean the empty columns of the PIN dataframe.
        Parameters:
            - df: DataFrame
        """

        df = df.withColumn(self.DOWNLOADED_INDEX, when(df[self.DOWNLOADED_INDEX] == "", None).otherwise(df[self.DOWNLOADED_INDEX]))
        df = df.withColumn(self.TAG_LIST_INDEX, when(df[self.TAG_LIST_INDEX] == "", None).otherwise(df[self.TAG_LIST_INDEX]))
        df = df.withColumn(self.INDEX_INDEX, when(df[self.INDEX_INDEX] == "", None).otherwise(df[self.INDEX_INDEX]))
        df = df.withColumn(self.IS_IMAGE_OR_VIDEO_INDEX, when(df[self.IS_IMAGE_OR_VIDEO_INDEX] == "", None).otherwise(df[self.IS_IMAGE_OR_VIDEO_INDEX]))
        df = df\
            .withColumn(self.SAVE_LOCATION_INDEX, when(df[self.SAVE_LOCATION_INDEX] == "", None)\
                .otherwise(when(df[self.SAVE_LOCATION_INDEX].startswith("Local save in ") == True, regexp_replace(self.SAVE_LOCATION_INDEX, "Local save in", ""))\
                    .otherwise(df[self.SAVE_LOCATION_INDEX])))
        df = df\
            .withColumn(self.FOLLOWER_COUNT_INDEX, when(df[self.FOLLOWER_COUNT_INDEX] == "", None)\
                .otherwise(when(df[self.FOLLOWER_COUNT_INDEX] == "User Info Error", None)\
                    .otherwise(when(df[self.FOLLOWER_COUNT_INDEX].endswith("k") == True, (regexp_replace(self.FOLLOWER_COUNT_INDEX, "k", "") * 10**3).cast(IntegerType()))\
                        .otherwise(when(df[self.FOLLOWER_COUNT_INDEX].endswith("M") == True, (regexp_replace(self.FOLLOWER_COUNT_INDEX, "M", "") * 10**6).cast(IntegerType()))\
                            .otherwise(df[self.FOLLOWER_COUNT_INDEX].cast(IntegerType()))))))
        df = df\
            .withColumn(self.POSTER_NAME_INDEX, when(df[self.POSTER_NAME_INDEX] == "", None)\
                .otherwise(when(df[self.POSTER_NAME_INDEX] == "User Info Error", None)\
                    .otherwise(df[self.POSTER_NAME_INDEX])))
        df = df\
            .withColumn(self.TITLE_INDEX, when(df[self.TITLE_INDEX] == "", None)\
                .otherwise(when(df[self.TITLE_INDEX] == "No Title Data Available", None)\
                    .otherwise(df[self.TITLE_INDEX])))
        df = df\
            .withColumn(self.DESCRIPTION_INDEX, when(df[self.DESCRIPTION_INDEX] == "", None)\
                .otherwise(when(df[self.DESCRIPTION_INDEX] == "No description available Story format", None)\
                    .otherwise(df[self.DESCRIPTION_INDEX])))
        df = df\
            .withColumn(self.IMAGE_SRC_INDEX, when(df[self.IMAGE_SRC_INDEX] == "", None)\
                .otherwise(when(df[self.IMAGE_SRC_INDEX] == "Image src error.", None)\
                    .otherwise(df[self.IMAGE_SRC_INDEX])))
            
        df = self.set_pin_columns_types(df)
        df = self.reorder_df_pin_columns(df)

        return df

pin_cleaner = SparkPinCleaner()
df = get_dataframe_from_drive("0affcd87e38f.pin")
df_pin_cleaned = pin_cleaner.clean_empty_df_pin(df)
display(df_pin_cleaned.orderBy(pin_cleaner.FOLLOWER_COUNT_INDEX, ascending=False))
dbutils.notebook.exit(df_pin_cleaned)