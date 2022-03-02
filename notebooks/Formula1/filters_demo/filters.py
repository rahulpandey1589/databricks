# Databricks notebook source
# MAGIC %run "../includes/configuration"

# COMMAND ----------

# MAGIC %scala
# MAGIC spark.conf.set(
# MAGIC     "fs.azure.account.key.databricksstaccount12345.dfs.core.windows.net",
# MAGIC     dbutils.secrets.get(scope="databricks-storage-scope",key="storage-account-key"))

# COMMAND ----------

races_df = spark.read.parquet(f"{processed_folder_path}/races").filter("race_year = 2019") \
.withColumnRenamed("name","race_name")

circuit_df = spark.read.parquet(f"{processed_folder_path}/circuits") \
.withColumnRenamed("name","circuit_name")


# COMMAND ----------

joined_df = circuit_df.join(races_df,circuit_df.circuitId == races_df.circuit_id,"inner") \
.select(circuit_df.circuit_name,circuit_df.location,circuit_df.country,races_df.race_name)

display(joined_df)

# COMMAND ----------

