# Databricks notebook source
# MAGIC %run "./clean-df-pin"

# COMMAND ----------

# MAGIC %run "./clean-df-user"

# COMMAND ----------

# MAGIC %md
# MAGIC What is the most popular category people post to based on the following age groups:
# MAGIC
# MAGIC   - 18-24
# MAGIC   - 25-35
# MAGIC   - 36-50
# MAGIC   - +50
# MAGIC
# MAGIC Your query should return a DataFrame that contains the following columns:
# MAGIC
# MAGIC   - age_group, a new column based on the original age column
# MAGIC   - category
# MAGIC   - category_count, a new column containing the desired query output

# COMMAND ----------


df_user_cleaned = df_user_cleaned.withColumnRenamed(user_cleaner.IND_INDEX, "user_ind")
df_join_pin_user = df_pin_cleaned\
    .join(df_user_cleaned, df_pin_cleaned.ind == df_user_cleaned.user_ind, "inner")

df_user_category_by_age_groups = df_join_pin_user\
    .select(user_cleaner.AGE_INDEX, "category")\
    .withColumn(user_cleaner.AGE_INDEX, when((df_join_pin_user.age >= 18) & (df_join_pin_user.age <= 24), lit("18-24"))\
        .otherwise(when((df_join_pin_user.age >= 25) & (df_join_pin_user.age <= 35), lit("25-35"))\
            .otherwise(when((df_join_pin_user.age >= 36) & (df_join_pin_user.age <= 50), lit("36-50"))\
                .otherwise(when(df_join_pin_user.age > 50, lit("+50"))))))\
    .groupBy(pin_cleaner.CATEGORY_INDEX, user_cleaner.AGE_INDEX)\
    .count()\
    .select(col(user_cleaner.AGE_INDEX).alias("age_group"), pin_cleaner.CATEGORY_INDEX, col("count").alias("category_count"))\
    .orderBy(col(pin_cleaner.CATEGORY_INDEX), ascending=False)

display(df_user_category_by_age_groups)