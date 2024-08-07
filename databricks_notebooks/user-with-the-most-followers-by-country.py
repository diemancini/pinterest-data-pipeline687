# Databricks notebook source
# MAGIC %run "./clean-df-pin"

# COMMAND ----------

# MAGIC %run "./clean-df-geo"

# COMMAND ----------

# MAGIC %run "./clean-df-user"

# COMMAND ----------

# MAGIC %md
# MAGIC Find the user with most followers in each country
# MAGIC
# MAGIC Step 1: For each country find the user with the most followers.
# MAGIC
# MAGIC Your query should return a DataFrame that contains the following columns:
# MAGIC
# MAGIC   - country
# MAGIC   - poster_name
# MAGIC   - follower_count
# MAGIC
# MAGIC Step 2: Based on the above query, find the country with the user with most followers.
# MAGIC
# MAGIC Your query should return a DataFrame that contains the following columns:
# MAGIC
# MAGIC - country
# MAGIC - follower_count
# MAGIC - This DataFrame should have only one entry.

# COMMAND ----------

# For each country find the user with the most followers

df_geo_cleaned = df_geo_cleaned.withColumnRenamed(geo_cleaner.IND_INDEX, "geo_ind")
df_user_cleaned = df_user_cleaned.withColumnRenamed(user_cleaner.IND_INDEX, "user_ind")
#display(df_user_cleaned)
df_join_pin_geo_user = df_pin_cleaned\
    .join(df_geo_cleaned, df_pin_cleaned.ind == df_geo_cleaned.geo_ind, "inner")\
    .join(df_user_cleaned, df_pin_cleaned.ind == df_user_cleaned.user_ind, "inner")

df_user_most_followers_by_country = df_join_pin_geo_user\
    .groupBy(geo_cleaner.COUNTRY_INDEX, col(user_cleaner.USER_NAME_INDEX).alias("poster_name"))\
    .agg(F.max(pin_cleaner.FOLLOWER_COUNT_INDEX).alias(pin_cleaner.FOLLOWER_COUNT_INDEX))\
    .orderBy(col(pin_cleaner.FOLLOWER_COUNT_INDEX), ascending=False)
display(df_user_most_followers_by_country)

# COMMAND ----------

df_user_most_follower = df_user_most_followers_by_country.limit(1)
display(df_user_most_follower)
dbutils.notebook.exit(df_user_most_followers)