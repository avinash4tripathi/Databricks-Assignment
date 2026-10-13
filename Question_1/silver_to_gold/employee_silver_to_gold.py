# Databricks notebook source
# MAGIC %run ../source_to_bronze/utils
# MAGIC

# COMMAND ----------

silver_path = (
    "/Volumes/databricks_assignment/default/assignment_volume"
    "/bronze_to_silver/employee_df"
) 

employee_gold_df = (
    spark.read
    .format("delta")
    .load(silver_path)
)

display(employee_gold_df)
employee_gold_df.printSchema()

# COMMAND ----------

#Department-wise total salary — descending order
from pyspark.sql.functions import (
    sum, count, avg, col, current_date
)

department_salary_df = (
    employee_gold_df
    .groupBy("department")
    .agg(
        sum("salary").alias("total_salary")
    )
    .orderBy(
        col("total_salary").desc()
    )
)

display(department_salary_df)

# COMMAND ----------

#Department aur country ke hisaab se employee count
employee_count_df = (
    employee_gold_df
    .groupBy("department", "country")
    .agg(
        count("EmployeeID").alias("employee_count")
    )
    .orderBy("department", "country")
)

display(employee_count_df)

# COMMAND ----------

#Department names aur corresponding country names
department_country_df = (
    employee_gold_df
    .select("department", "country")
    .distinct()
    .orderBy("department", "country")
)

display(department_country_df)

# COMMAND ----------

#Department-wise average employee age
average_age_df = (
    employee_gold_df
    .groupBy("department")
    .agg(
        avg("age").alias("average_age")
    )
    .orderBy("department")
)

display(average_age_df)

# COMMAND ----------

#Add at_load_date to Gold DataFrame
gold_df = employee_gold_df.withColumn(
    "at_load_date",
    current_date()
)

display(gold_df)

# COMMAND ----------

#Write Gold data in delta format 
gold_path = (
    "/Volumes/databricks_assignment/default/assignment_volume/"
    "gold/employee/fact_employee"
)

(
    gold_df.write
    .format("delta")
    .mode("overwrite")
    .option(
        "replaceWhere",
        f"at_load_date = '{spark.sql('SELECT current_date()').first()[0]}'"
    )
    .save(gold_path) 
)

print("Gold Delta dataset written successfully") 