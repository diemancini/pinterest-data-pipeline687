# Databricks notebook source
# MAGIC %run "./clean-df-pin"

# COMMAND ----------

# MAGIC %run "./clean-df-geo"

# COMMAND ----------

# MAGIC %md
# MAGIC Find how many posts each category had between 2018 and 2022.
# MAGIC
# MAGIC Your query should return a DataFrame that contains the following columns:
# MAGIC
# MAGIC   - post_year, a new column that contains only the year from the timestamp column
# MAGIC   - category
# MAGIC   - category_count, a new column containing the desired query output

# COMMAND ----------

#Find how many posts each category had between 2018 and 2022

df_geo_cleaned = df_geo_cleaned.withColumnRenamed(geo_cleaner.IND_INDEX, "geo_ind")
df_join_pin_geo = df_pin_cleaned\
    .join(df_geo_cleaned, (df_pin_cleaned.ind == df_geo_cleaned.geo_ind) & (year(geo_cleaner.TIMESTAMP_INDEX) >= 2018) & (year(geo_cleaner.TIMESTAMP_INDEX) <= 2022), "inner")

df_geo_pin_groupby_category_country = df_join_pin_geo\
    .select(year(geo_cleaner.TIMESTAMP_INDEX).alias("post_year"), pin_cleaner.CATEGORY_INDEX)\
    .groupBy("post_year", pin_cleaner.CATEGORY_INDEX)\
    .count()\
    .withColumnRenamed("count", "category_count")\
    .orderBy(desc("category_count"))

display(df_geo_pin_groupby_category_country)
