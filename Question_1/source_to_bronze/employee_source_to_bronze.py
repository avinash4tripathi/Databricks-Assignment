# Databricks notebook source
# MAGIC %run ./utils

# COMMAND ----------

employee_df = spark.read.csv(
    "/Volumes/workspace/default/employee_data/employee-details (1).csv",
    header=True,
    inferSchema=True,
)

department_df = spark.read.csv(
    "/Volumes/databricks_assignment/default/assignment_volume/Country-Q1.csv",
    header=True,
    inferSchema=True,
)
country_df = spark.read.csv(
    "/Volumes/databricks_assignment/default/assignment_volume/Employee-Q1.csv",
    header=True,
    inferSchema=True,
)

display(employee_df)
display(department_df)
display(country_df)

# COMMAND ----------

base_path = "/Volumes/databricks_assignment/default/assignment_volume"

display(dbutils.fs.ls(base_path))

# COMMAND ----------

employee_df = (
    spark.read
    .option("header", True)
    .option("inferSchema", True)
    .csv("/Volumes/databricks_assignment/default/assignment_volume/Employee-Q1.csv")
)

department_df = (
    spark.read
    .option("header", True)
    .option("inferSchema", True)
    .csv("/Volumes/databricks_assignment/default/assignment_volume/Department-Q1.csv")
)

country_df = (
    spark.read
    .option("header", True)
    .option("inferSchema", True)
    .csv("/Volumes/databricks_assignment/default/assignment_volume/Country-Q1.csv")
)

print("Employee schema:")
employee_df.printSchema()

print("Department schema:")
department_df.printSchema()

print("Country schema:")
country_df.printSchema()

# COMMAND ----------

base_path = "/Volumes/databricks_assignment/default/assignment_volume"

employee_bronze_path = f"{base_path}/source_to_bronze/employee_df"
department_bronze_path = f"{base_path}/source_to_bronze/department_df"
country_bronze_path = f"{base_path}/source_to_bronze/country_df"

write_csv(employee_df, employee_bronze_path)
write_csv(department_df, department_bronze_path)
write_csv(country_df, country_bronze_path)

print("All three datasets written to Bronze successfully!")

# COMMAND ----------

display(
    dbutils.fs.ls(
        f"{base_path}/source_to_bronze"
    )
)


# COMMAND ----------

