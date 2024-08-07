# Databricks notebook source
# MAGIC %run "./clean-df-pin"

# COMMAND ----------

# MAGIC %run "./clean-df-user"

# COMMAND ----------

# MAGIC %md
# MAGIC Find the median follower count of users that have joined between 2015 and 2020, based on which age group they are part of.
# MAGIC
# MAGIC Your query should return a DataFrame that contains the following columns:
# MAGIC
# MAGIC   - age_group, a new column based on the original age column
# MAGIC   - post_year, a new column that contains only the year from the timestamp column
# MAGIC   - median_follower_count, a new column containing the desired query output

# COMMAND ----------

df_user_cleaned = df_user_cleaned.withColumnRenamed(user_cleaner.IND_INDEX, "user_ind")
df_join_pin_user = df_pin_cleaned\
    .join(df_user_cleaned, df_pin_cleaned.ind == df_user_cleaned.user_ind, "inner")

df_median_follower_count_by_year_and_age_groups = df_join_pin_user\
    .select(pin_cleaner.FOLLOWER_COUNT_INDEX, user_cleaner.DATE_JOINED_INDEX, user_cleaner.AGE_INDEX)\
    .withColumn("post_year", when((year(user_cleaner.DATE_JOINED_INDEX) >= 2015) & (year(user_cleaner.DATE_JOINED_INDEX) <= 2020), year(user_cleaner.DATE_JOINED_INDEX)))\
    .withColumn(user_cleaner.AGE_INDEX, when((df_join_pin_user.age >= 18) & (df_join_pin_user.age <= 24), lit("18-24"))\
        .otherwise(when((df_join_pin_user.age >= 25) & (df_join_pin_user.age <= 35), lit("25-35"))\
            .otherwise(when((df_join_pin_user.age >= 36) & (df_join_pin_user.age <= 50), lit("36-50"))\
                .otherwise(when(df_join_pin_user.age > 50, lit("+50"))))))\
    .groupBy("post_year", col(user_cleaner.AGE_INDEX).alias("age_group"))\
    .agg(F.expr('percentile_approx(follower_count, 0.5)').alias("median_follower_count"))\
    .orderBy("post_year", "age_group")\

display(df_median_follower_count_by_year_and_age_groups)

