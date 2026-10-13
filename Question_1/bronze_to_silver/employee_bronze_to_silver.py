# Databricks notebook source
# MAGIC %run ../source_to_bronze/utils

# COMMAND ----------

#Define custom schema
from pyspark.sql.types import (
    StructType,
    StructField,
    IntegerType,
    StringType
)

employee_schema = StructType([
    StructField("EmployeeID", IntegerType(), True),
    StructField("EmployeeName", StringType(), True),
    StructField("Department", StringType(), True),
    StructField("Country", StringType(), True),
    StructField("Salary", IntegerType(), True),
    StructField("Age", IntegerType(), True)
])

department_schema = StructType([
    StructField("DepartmentID", StringType(), True),
    StructField("DepartmentName", StringType(), True)
])

country_schema = StructType([
    StructField("CountryCode", StringType(), True),
    StructField("CountryName", StringType(), True)
])

print("All three custom schemas defined successfully!")

# COMMAND ----------

base_path = "/Volumes/databricks_assignment/default/assignment_volume"

employee_bronze_df = (
    spark.read
    .option("header", True)
    .schema(employee_schema)
    .csv(f"{base_path}/source_to_bronze/employee_df")
)

department_bronze_df = (
    spark.read
    .option("header", True)
    .schema(department_schema)
    .csv(f"{base_path}/source_to_bronze/department_df")
)

country_bronze_df = (
    spark.read
    .option("header", True)
    .schema(country_schema)
    .csv(f"{base_path}/source_to_bronze/country_df")
)

employee_bronze_df.printSchema()
department_bronze_df.printSchema()
country_bronze_df.printSchema()

# COMMAND ----------

#Convert camel case into snake case
from pyspark.sql.functions import col

employee_silver_df = convert_columns_to_snake_case(
    employee_bronze_df
)

department_silver_df = convert_columns_to_snake_case(
    department_bronze_df
)

country_silver_df = convert_columns_to_snake_case(
    country_bronze_df
)

display(employee_silver_df)
employee_silver_df.printSchema()

# COMMAND ----------

# Add Load_Data
employee_silver_df = add_load_date(
    employee_bronze_df,
    "load_data"
)
display(employee_silver_df)

# COMMAND ----------

#Write the Silver Delta dataset
silver_path = (
    "/Volumes/databricks_assignment/default/assignment_volume"
    "/bronze_to_silver/employee_df"
)

write_delta(
    employee_silver_df,
    silver_path,
    mode="overwrite"
)

print("Employee Silver Delta data saved successfully")

# COMMAND ----------

# verify the silver
silver_check_df = (
    spark.read
    .format("delta")
    .load(silver_path)
)

display(silver_check_df)
silver_check_df.printSchema()
