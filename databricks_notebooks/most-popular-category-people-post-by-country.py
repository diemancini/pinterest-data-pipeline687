# Databricks notebook source
# MAGIC %run "./clean-df-pin"
# MAGIC

# COMMAND ----------

# MAGIC %run "./clean-df-geo"

# COMMAND ----------

# MAGIC %md
# MAGIC Find the most popular Pinterest category people post to based on their country.
# MAGIC
# MAGIC Your query should return a DataFrame that contains the following columns:
# MAGIC
# MAGIC - country
# MAGIC - category
# MAGIC - category_count, a new column containing the desired query output

# COMMAND ----------

df_geo_cleaned = df_geo_cleaned.withColumnRenamed(geo_cleaner.IND_INDEX, "geo_ind")
df_join_pin_geo = df_pin_cleaned\
    .join(df_geo_cleaned, df_pin_cleaned.ind == df_geo_cleaned.geo_ind, "inner")

df_geo_pin_groupby_category_country = df_join_pin_geo\
    .select(pin_cleaner.IND_INDEX, geo_cleaner.COUNTRY_INDEX, pin_cleaner.CATEGORY_INDEX)\
    .groupBy(pin_cleaner.CATEGORY_INDEX, geo_cleaner.COUNTRY_INDEX)\
    .count()\
    .withColumnRenamed("count", "category_count")\
    .orderBy(desc("category_count"))

display(df_geo_pin_groupby_category_country)