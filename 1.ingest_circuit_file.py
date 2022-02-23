# Databricks notebook source
# MAGIC %md
# MAGIC ### Ingest circuits.csv file

# COMMAND ----------

# MAGIC %scala
# MAGIC spark.conf.set(
# MAGIC     "fs.azure.account.key.databricksstaccount12345.dfs.core.windows.net",
# MAGIC     dbutils.secrets.get(scope="databricks-storage-scope",key="storage-account-key"))

# COMMAND ----------

rawfolderpath = "abfss://databricksfilecontainer@databricksstaccount12345.dfs.core.windows.net/raw"
processedFolderPath = "abfss://databricksfilecontainer@databricksstaccount12345.dfs.core.windows.net/processed"

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType,StringType,DoubleType

# COMMAND ----------

circuits_schema = StructType(fields=[StructField("circuitId",IntegerType(),False),
                                    StructField("circuitRef",StringType(),True),
                                    StructField("name",StringType(),True),
                                    StructField("location",StringType(),True),
                                    StructField("country",StringType(),True),
                                    StructField("lat",DoubleType(),True),
                                    StructField("lng",DoubleType(),True),
                                    StructField("alt",IntegerType(),True),
                                    StructField("url",StringType(),True)
])

# COMMAND ----------


circuitcsvpath =f"{rawfolderpath}/circuits.csv"

circuitdf = spark.read \
.option("header",True) \
.schema(circuits_schema) \
.csv(circuitcsvpath)

# COMMAND ----------

display(circuitdf)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Select Only The Required Columns

# COMMAND ----------

from pyspark.sql.functions import col

# COMMAND ----------

circuit_select_df = circuitdf.select(col("circuitId"),col("circuitRef"),col("name"), col("location"),col("country"), col("lat"), col("lng"), col("alt"))

# COMMAND ----------

display(circuit_select_df)

# COMMAND ----------

# MAGIC %md 
# MAGIC 
# MAGIC ### Rename Selected Columns

# COMMAND ----------

circuits_renamed_df = circuit_select_df.withColumnRenamed("circuitiId","circuit_id") \
.withColumnRenamed("circuitRef","circuit_ref") \
.withColumnRenamed("lat","latitude") \
.withColumnRenamed("lng","longitude") \
.withColumnRenamed("alt","altitude") \


display(circuits_renamed_df)



# COMMAND ----------

# MAGIC %md
# MAGIC ### Add new column into results

# COMMAND ----------

from pyspark.sql.functions import current_timestamp

# COMMAND ----------

circuits_final_df = circuits_renamed_df.withColumn("ingestion_date",current_timestamp()) 
display(circuits_final_df)

# COMMAND ----------

# MAGIC %md 
# MAGIC 
# MAGIC ### Write data into filesystem using Parquet Format

# COMMAND ----------

circuits_final_df.write.mode("overwrite").parquet(f"{processedFolderPath}/circuits")

# COMMAND ----------

display(spark.read.parquet(f"{processedFolderPath}/circuits"))

# COMMAND ----------


