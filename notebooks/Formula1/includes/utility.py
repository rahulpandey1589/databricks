# Databricks notebook source
# MAGIC %md
# MAGIC 
# MAGIC This includes common functions that could be used across complete project

# COMMAND ----------

from pyspark.sql.functions import current_timestamp

def add_ingestion_date(input_df):
    output_df = input_df.withColumn("ingestion_date",current_timestamp())
    return output_df
    

# COMMAND ----------

