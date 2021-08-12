# Databricks notebook source
# MAGIC %run ./dlt

# COMMAND ----------

# MAGIC %run ./dlt_workflow_refactored

# COMMAND ----------

from pyspark.sql import Row
import unittest

# COMMAND ----------

from pyspark.sql.functions import lit, col, when, array, size
import datetime


timestamp = datetime.datetime.fromisoformat("2000-01-01T00:00:00")
columns = [
  "airport_id", "name", "city", "country", "iata", "icao",
  "latitude", "longitude", "altitude", "timezone", "dst", "tz",
  "type", "source"
]


def timestamp_provider():
  return lit(timestamp)

def create_row(i, m, n):
  return Row(
    airport_id=str(i),
    name=f"Airport{i}",
    city=f"City{i}",
    country=f"Country{i%m}",
    iata=f"TST{i}",
    icao=f"TEST{i}",
    latitude="0.0",
    longitude="0.0",
    altitude="0",
    timezone="0",
    dst=f"{i}",
    tz=f"GMT+{i}" if i % m != n else None,
    type="airport",
    source="Test"
  )

def load_test_data():
  return spark.createDataFrame([create_row(i, 3, 2) for i in range(4)]).cache()  

def load_test_silver_data():
  return load_test_data().withColumn("nulls", when(col("tz").isNull(), array(lit(11))).otherwise(array())).cache()

def load_test_silver_clean_data():
  return load_test_silver_data().filter(size("nulls") == 0).cache()

# COMMAND ----------

class IntegrationTests(unittest.TestCase):
  test_context = {}
  
  @classmethod
  def setUpClass(cls):
    container.register(
      load_data_file=load_test_data,
      load_bronze_data=load_test_data,
      load_silver_data=load_test_silver_data,
      load_silver_data_clean=load_test_silver_clean_data,
      timestamp_provider=timestamp_provider
    )
  
  def test_bronze_airport_data(self):
    df = bronze_airport_data()
    result = df.collect()
    self.assertEqual(4, len(result), "Four records expected")
    for c in columns:
      self.assertIn(c, df.columns, f"Column '{c}' is not present")
    self.assertIn("ingest_timestamp", df.columns, "Column 'ingest_timestamp' is not present")
    self.assertIn("ingest_source", df.columns, "Column 'ingest_source' is not present")
    self.assertEqual(url.split("/")[-1], result[0].ingest_source, "Ingest source not correct")
    self.assertEqual(timestamp, result[0].ingest_timestamp, "Ingest timestamp not correct")
  
  def test_silver_airport_data(self):
    df = silver_airport_data()
    result = df.collect()
    self.assertEqual(4, len(result), "Four records expected")
    for c in columns:
      self.assertIn(c, df.columns, f"Column '{c}' is not present")
  
  def test_silver_airport_data_nulls(self):
    df = silver_airport_data_nulls()
    result = df.collect()
    self.assertEqual(1, len(result), "One record expected")
    self.assertIn(11, result[0].nulls, "Null column index expected")
  
  def test_silver_airport_data_clean(self):
    df = silver_airport_data_clean()
    result = df.collect()
    self.assertEqual(3, len(result), "Three records expected")
  
  def test_gold_airports_by_country(self):
    df = gold_airports_by_country()
    result = df.collect()
    self.assertEqual(2, len(result), "Two records expected")
    self.assertIn("processed_timestamp", df.columns, "Column 'processed_timestamp' is not present")
    self.assertEqual(timestamp, result[0].processed_timestamp, "Processed timestamp not correct")
    self.assertIn("country", df.columns, "Column 'country' is not present")
    self.assertIn("count", df.columns, "Column 'count' is not present")
    d = {r[0]: r[1] for r in result}
    self.assertEqual(2, d.get("Country0", -1), "Country0 count should be 2")
    self.assertEqual(1, d.get("Country1", -1), "Country1 count should be 1")
    
