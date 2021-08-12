# Databricks notebook source
# MAGIC %md # Unit Tests for Workflow
# MAGIC This notebook contains the unit tests for the Workflow

# COMMAND ----------

# MAGIC %md ## Imports

# COMMAND ----------

from pyspark.sql import Row

import unittest
import uuid

# COMMAND ----------

# MAGIC %md ## Call the Workflow notebook
# MAGIC 
# MAGIC The next cell will execute the Workflow notebook. This will create all of the classes and other definitions in the scope of this notebook so we can reference them later.

# COMMAND ----------

# MAGIC %run ./workflow

# COMMAND ----------

# MAGIC %md ## Create some mock classes
# MAGIC To promote well structured and testable code, we have mocked the FileAccess class so we can abstract the implementation of environment specific details from the system under test. In this case, we're interested in testing the **Workflow** class and not whether Databricks can read data from storage.

# COMMAND ----------

class MockFileAccess(FileAccess):
  def read(self, path: str) -> DataFrame:
    self.source = self.spark.createDataFrame([Row(id=1, name='Item 1', value=str(uuid.uuid4()))])
    return self.source.cache()
    
  def write(self, path: str, df: DataFrame) -> None:
    self.target = df
    

# COMMAND ----------

# MAGIC %md ## Create our tests class
# MAGIC The following class contains our unit tests we wish to execute to prove the Workflow class functions as expeceted.

# COMMAND ----------

class WorkflowTests(unittest.TestCase):
  @classmethod
  def setUpClass(cls):
    files = MockFileAccess(spark)
    workflow = Workflow(spark, files)
    workflow.run()    
    cls.source = files.source.collect()
    cls.records = files.target.collect()
    
  def test_record_count(self):
    self.assertEqual(1, len(self.records), 'There should be a single row')
    
  def test_name_transformation(self):
    self.assertEqual('ITEM 1', self.records[0].name, 'The name should be uppercase')
    
  def test_input_and_output_value_matches(self):
    self.assertEqual(self.source[0].value, self.records[0].value, 'The value should match')
    
  def test_record_is_processed(self):
    self.assertEqual(1, self.records[0].processed, 'The record should be processed')

