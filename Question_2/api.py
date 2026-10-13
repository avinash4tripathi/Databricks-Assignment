# Databricks notebook source
import requests
import json

from pyspark.sql.types import *
from pyspark.sql.functions import *


# COMMAND ----------

import requests

all_data = []
page = 1

while True:
    url = f"https://reqres.in/api/users?page={page}"
    response = requests.get(url)
    data = response.json()
    users = data.get("data", [])
    if not users:
        break
    all_data.extend(users)
    page += 1

print(f"Total users fetched: {len(all_data)}")

# COMMAND ----------

from pyspark.sql.types import *
schema = StructType([
    StructField("id", IntegerType(), True),
    StructField("email", StringType(), True),
    StructField("first_name", StringType(), True),
    StructField("last_name", StringType(), True),
    StructField("gender", StringType(), True),
    StructField("phone", StringType(), True),
    StructField("age", IntegerType(), True),
    StructField("address", StructType([
        StructField("address", StringType(), True),
        StructField("city", StringType(), True),
        StructField("state", StringType(), True)
    ]), True)
])

df = spark.createDataFrame(all_data, schema)
display(df)

# COMMAND ----------

# Flatten the nested 'address' struct column into individual columns
from pyspark.sql.functions import col

flattened_df = df.select(
    col("id"),
    col("email"),
    col("first_name"),
    col("last_name"),
    col("gender"),
    col("phone"),
    col("age"),
    col("address.address").alias("address"),
    col("address.city").alias("city"),
    col("address.state").alias("state")
)

display(flattened_df)

# COMMAND ----------

from pyspark.sql.functions import split

flattened_df = flattened_df.withColumn("site_address", split(col("email"), "@").getItem(1))

display(flattened_df)

# COMMAND ----------

from pyspark.sql.functions import current_date

flattened_df = flattened_df.withColumn("load_date", current_date())

display(flattened_df)

# COMMAND ----------

flattened_df.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .save("/Volumes/databricks_assignment/default/assignment_volume/site_info/person_info")

print("Data written to /Volumes/databricks_assignment/default/assignment_volume/site_info/person_info in Delta format (overwrite mode).")