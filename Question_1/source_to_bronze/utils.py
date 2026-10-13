# Databricks notebook source
from pyspark.sql.functions import current_date
from pyspark.sql.types import StringType
from pyspark.sql.functions import udf


# Convert CamelCase column names to snake_case
def camel_to_snake(column_name):
    result = ""

    for char in column_name:
        if char.isupper():
            result += "_" + char.lower()
        else:
            result += char

    return result.strip("_")


# Register the UDF
camel_to_snake_udf = udf(
    camel_to_snake, StringType()
)


# Rename DataFrame columns to snake_case
def convert_columns_to_snake_case(df):
    for old_column in df.columns:
        new_column = camel_to_snake(old_column)

        if old_column != new_column:
            df = df.withColumnRenamed(
                old_column, new_column
            )

    return df


# Add a load date column
def add_load_date(df, column_name="load_date"):
    return df.withColumn(
        column_name, current_date()
    )


# Write DataFrame as CSV
def write_csv(df, path, mode="overwrite"):
    (
        df.write
        .format("csv")
        .option("header", True)
        .mode(mode)
        .save(path)
    )


# Write DataFrame as Delta
def write_delta(df, path, mode="overwrite"):
    (
        df.write
        .format("delta")
        .mode(mode)
        .save(path)
    )


print("All utility functions defined successfully!")

# COMMAND ----------

