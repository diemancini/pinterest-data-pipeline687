# Databricks notebook source
# MAGIC %run "./clean-df-user"

# COMMAND ----------

# MAGIC %md
# MAGIC Find how many users have joined between 2015 and 2020.
# MAGIC
# MAGIC Your query should return a DataFrame that contains the following columns:
# MAGIC
# MAGIC   - post_year, a new column that contains only the year from the timestamp column
# MAGIC   - number_users_joined, a new column containing the desired query output

# COMMAND ----------

# Find how many users have joined between 2015 and 2020.

df_user_joined_count_by_year = df_user_cleaned\
    .select(user_cleaner.DATE_JOINED_INDEX)\
    .withColumn("post_year", when((year(user_cleaner.DATE_JOINED_INDEX) >= 2015) & (year(user_cleaner.DATE_JOINED_INDEX) <= 2020), year(user_cleaner.DATE_JOINED_INDEX)))\
    .groupBy("post_year")\
    .count()\
    .withColumnRenamed("count", "number_users_joined")\
    .orderBy(desc("post_year"))

display(df_user_joined_count_by_year)

