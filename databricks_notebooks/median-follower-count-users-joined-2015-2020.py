# Databricks notebook source
# MAGIC %run "./clean-df-user"

# COMMAND ----------

# MAGIC %run "./clean-df-pin"

# COMMAND ----------

# MAGIC %md
# MAGIC Find the median follower count of users have joined between 2015 and 2020.
# MAGIC
# MAGIC Your query should return a DataFrame that contains the following columns:
# MAGIC
# MAGIC   - post_year, a new column that contains only the year from the timestamp column
# MAGIC   - median_follower_count, a new column containing the desired query output

# COMMAND ----------

# Find the median follower count of users have joined between 2015 and 2020

df_user_cleaned = df_user_cleaned.withColumnRenamed(user_cleaner.IND_INDEX, "user_ind")
df_join_pin_user = df_pin_cleaned\
    .join(df_user_cleaned, df_pin_cleaned.ind == df_user_cleaned.user_ind, "inner")

df_median_follower_count_user_by_year = df_join_pin_user\
    .select(pin_cleaner.FOLLOWER_COUNT_INDEX, user_cleaner.DATE_JOINED_INDEX)\
    .withColumn("post_year", when((year(user_cleaner.DATE_JOINED_INDEX) >= 2015) & (year(user_cleaner.DATE_JOINED_INDEX) <= 2020), year(user_cleaner.DATE_JOINED_INDEX)))\
    .groupBy("post_year")\
    .agg(F.expr('percentile_approx(follower_count, 0.5)').alias("median_follower_count"))\
    .orderBy("post_year")

display(df_median_follower_count_user_by_year)
    
