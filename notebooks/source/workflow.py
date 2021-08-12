# Databricks notebook source
# MAGIC %md # Workflow
# MAGIC This notebook defines a sample data processing workflow as a set of classes. To execute the workflow, use a **driver** notebook and to test the workflow use a **tests** notebook. This promotes good coding practices around code structure and code coverage while still working within a Notebook environment.

# COMMAND ----------

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import expr, col
from pyspark.sql.types import StructType, StructField, IntegerType, StringType

# COMMAND ----------

# MAGIC %md ## File Access
# MAGIC This is an abstract class that forms the interface to read/write DataFrames used by the pipeline. This allows control of the input/output file systems and files for unit testing and allows tests to execute on totally separate data in all environments.

# COMMAND ----------

class FileAccess:
  def __init__(self, spark: SparkSession) -> None:
    self.spark = spark
  def read(self, path: str) -> DataFrame:
    pass
  def write(self, path: str, df: DataFrame) -> None:
    pass

# COMMAND ----------

# MAGIC %md ## DBFS File Access
# MAGIC This class is a concrete implementation of the FileAccess class and is provided for the production pipeline. Data is read from and written to DBFS.

# COMMAND ----------

class DbfsFileAccess(FileAccess):
  def read(self, path: str) -> DataFrame:
    schema=StructType([
      StructField('id', IntegerType(), False), 
      StructField('name', StringType(), False), 
      StructField('value', StringType(), False)
    ])
    return spark.read.format('csv').load(f'dbfs:/tmp/pipeline/{path}', schema=schema, header=True)
  def write(self, path: str, df: DataFrame):
    df.write.format('delta').mode('overwrite').save(f'dbfs:/tmp/pipeline/{path}')

# COMMAND ----------

# MAGIC %md ## Data Workflow
# MAGIC The following class implements the data workflow

# COMMAND ----------

class Workflow:
  def __init__(self, spark: SparkSession, files: FileAccess) -> None:
    self.spark = spark
    self.files = files  
  
  def _process(self, df: DataFrame) -> DataFrame:
    df = df.select(
      col('id'), 
      expr('UPPER(name) AS name'), 
      col('value'), 
      expr('CAST(1 AS TINYINT) AS processed'), 
      expr('CURRENT_TIMESTAMP() AS processed_time')
    )
    return df
  
  def run(self) -> None:
    df = self.files.read('raw.csv')
    df = df.transform(self._process)
    self.files.write('refined', df)
