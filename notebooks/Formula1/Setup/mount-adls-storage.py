# Databricks notebook source
# MAGIC %md
# MAGIC ### Intial Setup - Making connection to Azure Storage Account

# COMMAND ----------

# MAGIC %scala
# MAGIC spark.conf.set(
# MAGIC     "fs.azure.account.key.databricksstaccount12345.dfs.core.windows.net",
# MAGIC     dbutils.secrets.get(scope="databricks-storage-scope",key="storage-account-key"))

# COMMAND ----------

def mount_adls(container_name):
    storageAccountName = "databricksstaccount12345"
    try:
        spark.conf.set("fs.azure.createRemoteFileSystemDuringInitialization", "true")
        dbutils.fs.ls(f"abfss://{container_name}@{storageAccountName}.dfs.core.windows.net/")
        spark.conf.set("fs.azure.createRemoteFileSystemDuringInitialization", "false")
    except Exception as e:
      print(f"{e}")

# COMMAND ----------

mount_adls("databricksfilecontainer")

# COMMAND ----------

rawfolderpath = "abfss://databricksfilecontainer@databricksstaccount12345.dfs.core.windows.net/raw"
processedFolderPath = "abfss://databricksfilecontainer@databricksstaccount12345.dfs.core.windows.net/processed"

circuitcsvpath =f"{rawfolderpath}/circuits.csv"

circuitdf = spark.read.csv(circuitcsvpath)

circuitdf.show()

# COMMAND ----------

