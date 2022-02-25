# Databricks notebook source
# MAGIC %md
# MAGIC ### Ingest Data From Race.csv

# COMMAND ----------

# MAGIC %scala
# MAGIC spark.conf.set(
# MAGIC     "fs.azure.account.key.databricksstaccount12345.dfs.core.windows.net",
# MAGIC     dbutils.secrets.get(scope="databricks-storage-scope",key="storage-account-key"))

# COMMAND ----------

rawfolderpath = "abfss://databricksfilecontainer@databricksstaccount12345.dfs.core.windows.net/raw"
processedFolderPath = "abfss://databricksfilecontainer@databricksstaccount12345.dfs.core.windows.net/processed"

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, StringType,DateType

# COMMAND ----------

race_schema = StructType(fields=[StructField("raceId",IntegerType(),False),
                                 StructField("year",IntegerType(),False),
                                 StructField("round",IntegerType(),False),
                                 StructField("circuitId",IntegerType(),False),
                                 StructField("name",StringType(),False),
                                 StructField("date",DateType(),False),
                                 StructField("time",StringType(),False),
                                 StructField("url",StringType(),False)

])

# COMMAND ----------

racecsvpath = f"{rawfolderpath}/races.csv"
race_df = spark.read.option("Header",True).option("schema",race_schema).csv(racecsvpath)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Transformation : Select Only Required Columns

# COMMAND ----------

from pyspark.sql.functions import col,lit,concat,to_timestamp,current_timestamp


# COMMAND ----------

race_selected_col = race_df.select(col("raceId"),col("year"),col("round"),col("circuitId"),col("name"),col("date"),col("time"))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Transformation : Rename Columns 
# MAGIC * Rename year to race_year
# MAGIC * Rename circuitid to circuit_id
# MAGIC * Added race_timestamp to display combine values of date and time, remove date and time from end result 
# MAGIC * Insert new column Ingestion Date 

# COMMAND ----------

race_tranformed_col = race_selected_col.withColumnRenamed("raceId","race_id") \
.withColumnRenamed("year","race_year") \
.withColumnRenamed("circuitid","circuit_id") \
.withColumn("race_timestamp",to_timestamp(concat(col("date"),lit(" "),col("time")),"yyyy-MM-dd HH:mm:ss")) \
.withColumn("ingestion_date",current_timestamp())


# COMMAND ----------

# MAGIC %md
# MAGIC ## Remove Date and Time column in final result

# COMMAND ----------

final_result = race_tranformed_col.select(col("race_id"),col("race_year"),col("round"),col("circuit_id"),col("name"),col("race_timestamp"),col("ingestion_date"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Write data into Parquet Format in Processed Folder

# COMMAND ----------

final_parquet_output = final_result.write.mode("overwrite").parquet(f"{processedFolderPath}/circuits")

# COMMAND ----------

# MAGIC %md 
# MAGIC # Display Final Output

# COMMAND ----------

display(spark.read.parquet(f"{processedFolderPath}/circuits"))

# COMMAND ----------

