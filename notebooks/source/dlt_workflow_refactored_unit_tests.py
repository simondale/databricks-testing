# Databricks notebook source
# MAGIC %run ./dlt

# COMMAND ----------

# MAGIC %run ./dlt_workflow_refactored

# COMMAND ----------

from pyspark.sql import Row
import unittest

# COMMAND ----------

from pyspark.sql.functions import lit
import datetime


timestamp = datetime.datetime.fromisoformat("2000-01-01T00:00:00")


def timestamp_provider():
  return lit(timestamp)

# COMMAND ----------

from pyspark.sql.functions import when, col
from pyspark.sql import Row


class FunctionUnitTests(unittest.TestCase):
  @classmethod
  def setUpClass(cls):
    container.register(
      timestamp_provider=timestamp_provider
    )
  
  def test_add_ingest_columns(self):
    df = spark.range(1)
    df = df.transform(container.add_ingest_columns)
    result = df.collect()
    self.assertEqual(1, len(result), "Only one record expected")
    self.assertIn("ingest_timestamp", df.columns, "Ingest timestamp column not present")
    self.assertIn("ingest_source", df.columns, "Ingest source column not present")
    self.assertEqual(url.split("/")[-1], result[0].ingest_source, "Ingest source not correct")
    self.assertEqual(timestamp, result[0].ingest_timestamp, "Ingest timestamp not correct")
    
  def test_add_processed_timestamp(self):
    df = spark.range(1)
    df = df.transform(container.add_processed_timestamp)
    result = df.collect()
    self.assertEqual(1, len(result), "Only one record expected")
    self.assertIn("processed_timestamp", df.columns, "Processed timestamp column not present")
    self.assertEqual(timestamp, result[0].processed_timestamp, "Processed timestamp not correct")
    
  def test_add_null_index_array(self):
    df = spark.createDataFrame([
      Row(id=1, test_null=None),
      Row(id=2, test_null=1)
    ])
    df = df.transform(container.add_null_index_array)
    result = df.collect()
    self.assertEqual(2, len(result), "Two records are expected")    
    self.assertIn("nulls", df.columns, "Nulls column not present")
    self.assertIsNone(result[0].test_null, "First record should contain null")
    self.assertIsNotNone(result[1].test_null, "Second record should not contain null")
    self.assertIn(1, result[0].nulls, "Nulls array should include 1")
    self.assertIsNot(result[1].nulls, "Nulls array should be empty")
    
  def test_filter_null_index_empty(self):
    df = spark.createDataFrame([
      Row(id=1, test_null=None, nulls=[1]),
      Row(id=2, test_null=1, nulls=[])
    ])
    df = df.transform(container.filter_null_index_empty)
    result = df.collect()
    self.assertEqual(1, len(result), "One record is expected")
    self.assertNotIn("nulls", df.columns, "Nulls column not present")
    
  def test_filter_null_index_not_empty(self):
    df = spark.createDataFrame([
      Row(id=1, test_null=None, nulls=[1]),
      Row(id=2, test_null=1, nulls=[])
    ])
    df = df.transform(container.filter_null_index_not_empty)
    result = df.collect()
    self.assertEqual(1, len(result), "One record is expected")
    self.assertIn("nulls", df.columns, "Nulls column not present")
    
  def test_agg_count_by_country(self):
    df = spark.createDataFrame([
      Row(country="Country0"),
      Row(country="Country1"),
      Row(country="Country0")
    ])
    df = df.transform(container.agg_count_by_country)
    result = df.collect()
    self.assertEqual(2, len(result), "Two records expected")
    self.assertIn("country", df.columns, "Country column not present")
    self.assertIn("count", df.columns, "Count column not present")
    d = {r[0]: r[1] for r in result}
    self.assertEqual(2, d.get("Country0", -1), "Country0 count should be 2")
    self.assertEqual(1, d.get("Country1", -1), "Country1 count should be 1")

