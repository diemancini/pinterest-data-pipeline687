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
# MAGIC To clean the df_user DataFrame you should perform the following transformations:
# MAGIC
# MAGIC - Create a new column user_name that concatenates the information found in the first_name and last_name columns
# MAGIC - Drop the first_name and last_name columns from the DataFrame
# MAGIC - Convert the date_joined column from a string to a timestamp data type
# MAGIC - Reorder the DataFrame columns to have the following column order:
# MAGIC    - ind
# MAGIC    - user_name
# MAGIC    - age
# MAGIC    - date_joined

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql import functions as F
from pyspark.sql.types import ( 
    StringType, IntegerType
)

class SparkUserCleaner:

    IND_INDEX = "ind"
    AGE_INDEX = "age"
    DATE_JOINED_INDEX = "date_joined"
    FIRST_NAME_INDEX = "first_name"
    LAST_NAME_INDEX = "last_name"
    USER_NAME_INDEX = "user_name"

    def reorder_df_user_columns(self, df):
        """
        Reorder the columns of the USER dataframe.
        Parameters:
            - df: DataFrame
        """

        df = df.select(
            self.IND_INDEX,
            self.USER_NAME_INDEX,
            self.AGE_INDEX,
            self.DATE_JOINED_INDEX
        )

        return df

    def clean_empty_df_user(self, df: DataFrame):
        """
        Clean the empty columns of the USER dataframe.
        Parameters:
            - df: DataFrame
        """
        
        df = df\
            .withColumn(self.USER_NAME_INDEX, F.concat(self.FIRST_NAME_INDEX, F.lit(" "), self.LAST_NAME_INDEX))\
                .drop(self.FIRST_NAME_INDEX, self.LAST_NAME_INDEX)
        df = df\
            .withColumn(self.DATE_JOINED_INDEX, col(self.DATE_JOINED_INDEX).cast('timestamp'))
            
        df = self.reorder_df_user_columns(df)

        return df

user_cleaner = SparkUserCleaner()
df = get_dataframe_from_drive("0affcd87e38f.user")
print(df.columns)
df_user_cleaned = user_cleaner.clean_empty_df_user(df)
df_user_cleaned.printSchema()
display(df_user_cleaned)
dbutils.notebook.exit(df_user_cleaned)
