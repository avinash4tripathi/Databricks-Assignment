# Databricks notebook source
#Fetch the data with API
import requests

all_data = []
page = 1

while True:
    url = "https://reqres.in/api/users"

    response = requests.get(
        url,
        params={"page": page},
        timeout=30
    )

    response.raise_for_status()
    result = response.json()

    users = result.get("data", [])

    if not users:
        break

    all_data.extend(users)
    page += 1

print("Total users fetched:", len(all_data))
print("Total pages requested:", page - 1)

# COMMAND ----------

# Create Dataframe with Custom Schema
from pyspark.sql.types import (
    StructType, StructField,
    IntegerType, StringType
)

user_schema = StructType([
    StructField("id", IntegerType(), True),
    StructField("firstName", StringType(), True),
    StructField("lastName", StringType(), True),
    StructField("email", StringType(), True),
    StructField("phone", StringType(), True),
    StructField("age", IntegerType(), True),
    StructField("gender", StringType(), True),
    StructField(
        "address",
        StructType([
            StructField("address", StringType(), True),
            StructField("city", StringType(), True),
            StructField("state", StringType(), True)
        ]),
        True
    )
])

df = spark.createDataFrame(
    all_data,
    schema=user_schema
)

display(df)
df.printSchema()

# COMMAND ----------

# flatten the Nested adress 
from pyspark.sql.functions import col

df = df.select(
    col("id"),
    col("firstName"),
    col("lastName"),
    col("email"),
    col("phone"),
    col("age"),
    col("gender"),
    col("address.address").alias("address"),
    col("address.city").alias("city"),
    col("address.state").alias("state")
)

display(df)


# COMMAND ----------

# Extract site adrees from the mail
from pyspark.sql.functions import split

df_flat = df.withColumn(
    "site_address",
    split(col("email"), "@")[1]
)

display(df_flat)

# COMMAND ----------

#Add load_data
from pyspark.sql.functions import current_date

df_flat = df_flat.withColumn(
    "load_date",
    current_date()
)

display(df_flat)

# COMMAND ----------

# We store the data in delta format using a catalog-qualified table name
target_table = "databricks_assignment.default.site_info_person"

(
    df_flat.write
    .format("delta")
    .mode("overwrite")
    .option("mergeSchema", "true")
    .saveAsTable(target_table)
)

print("Question 2 data saved successfully!")

# COMMAND ----------

#Now Verify the data is saved in the table
result_df = (
    spark.read
    .table(target_table)
)

display(result_df)

# COMMAND ----------

# Inspect one API record
print(all_data[0])

# Check the keys available in the API response
print(all_data[0].keys())

# COMMAND ----------

# Verify the API fields and custom schema.
print("Address field:", all_data[0].get("address"))
print("Email field:", all_data[0].get("email"))

# COMMAND ----------

from pyspark.sql.types import (
    StructType, StructField,
    IntegerType, StringType
)

user_schema = StructType([
    StructField("id", IntegerType(), True),
    StructField("email", StringType(), True),
    StructField("first_name", StringType(), True),
    StructField("last_name", StringType(), True),
    StructField("avatar", StringType(), True)
])

df = spark.createDataFrame(
    all_data,
    schema=user_schema
)

display(df)
df.printSchema()

# COMMAND ----------

# Remove the Unwanted API metadata 
df_flat = df.select(
    "id",
    "first_name",
    "last_name",
    "email",
    "avatar"
)

display(df_flat)

# COMMAND ----------

# I make the site adrees from the email
from pyspark.sql.functions import split, col

df_flat = df_flat.withColumn(
    "site_address",
    split(col("email"), "@")[1]
)

display(df_flat)

# COMMAND ----------

# Add the load data
from pyspark.sql.functions import current_date

df_flat = df_flat.withColumn(
    "load_date",
    current_date()
)

display(df_flat)

# COMMAND ----------

#Save into delta format
target_table = "databricks_assignment.default.site_info_person"

(
    df_flat.write
    .format("delta")
    .mode("overwrite")
    .option("mergeSchema", "true")
    .saveAsTable(target_table)
)

print("API data saved successfully!")

# COMMAND ----------

# Save the delta target path
result_df = (
    spark.read
    .table(target_table)
)

display(result_df)

# COMMAND ----------

#Verify final Delta DataFrame
final_df = (
    spark.read
    .table(target_table)
)

display(final_df)

print("Total records:", final_df.count())
final_df.printSchema()

# COMMAND ----------

# valodate the email domain
from pyspark.sql.functions import col

display(
    final_df.select(
        "email",
        "site_address"
    )
)

# COMMAND ----------

final_df.printSchema()
display(final_df.limit(5))

# COMMAND ----------

print("Columns:", final_df.columns)
print("Total records:", final_df.count())

# COMMAND ----------

from pyspark.sql.functions import col, split, current_date

# Select the actual fields returned by the API
df_corrected = final_df.select(
    col("id"),
    col("first_name"),
    col("last_name"),
    col("email"),
    col("avatar")
)

# Derive the email domain
df_corrected = df_corrected.withColumn(
    "site_address",
    split(col("email"), "@").getItem(1)
)

# Add the current load date
df_corrected = df_corrected.withColumn(
    "load_date",
    current_date()
)

display(df_corrected)

# COMMAND ----------

target_path = (
    "/Volumes/databricks_assignment/default/assignment_volume/"
    "site_info/person_info"
)

(
    df_corrected.write
    .format("delta")
    .mode("overwrite")
    .save(target_path)
)

print("Corrected Delta data saved successfully!")

# COMMAND ----------

final_check = (
    spark.read
    .format("delta")
    .load(target_path)
)

display(final_check)
print("Total records:", final_check.count())

# COMMAND ----------

# verify the final output
target_path = (
    "/Volumes/databricks_assignment/default/assignment_volume/"
    "site_info/person_info"
)

final_check = (
    spark.read
    .format("delta")
    .load(target_path)
)

display(final_check)
final_check.printSchema()

print("Total records:", final_check.count())

# COMMAND ----------

