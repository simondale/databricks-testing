# Databricks notebook source
# MAGIC %run ./workflow

# COMMAND ----------

try:
  files = DbfsFileAccess(spark)
  workflow = Workflow(spark, files)
  workflow.run()
except Exception as e:
  print(e)
  raise

# COMMAND ----------


