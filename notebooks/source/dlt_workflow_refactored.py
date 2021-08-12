# Databricks notebook source
import inspect


class DIContainer:
  def __init__(self, **kwargs):
    self.dependencies = {}
    for k, v in kwargs.items():
      self._register(k, v)      
                       
  def register(self, **kwargs):
    for k, v in kwargs.items():
      self._register(k, v)  
    
  def resolve(self, key, *args, **kwargs):
    value = self.dependencies.get(key)
    if callable(value):
      spec = inspect.getfullargspec(value)
      args_dict = {}
      for arg in spec[0]:
        a = self.resolve(arg)
        if a is not None:
          args_dict[arg] = a
      args_dict.update(kwargs)
      return self._create_wrapper(value, spec[0], args, args_dict)
    else:
      return value
  
  def _register(self, key, value):
    self.dependencies[key] = value
    self.__setattr__(f"get_{key}", lambda *a, **k: self.resolve(key, *a, **k))
    setattr(DIContainer, key, property(lambda s: self.resolve(key)))
    
  @staticmethod
  def _create_wrapper(x, spec, args_list, args_dict):
    if [s for s in spec if s not in args_dict]:
      def wrapper(*args):
        args += args_list
        return x(*args, **args_dict)
      return wrapper
    else:
      def wrapper():
        return x(*args_list, **args_dict)
      return wrapper

# COMMAND ----------

import dlt

container = DIContainer()


@dlt.table(comment="Bronze airports data")
def bronze_airport_data():
  return container.load_data_file().transform(container.add_ingest_columns)


@dlt.table(comment="Bronze airports data")
def silver_airport_data():
  return container.load_bronze_data().transform(container.add_null_index_array)


@dlt.table(comment="Silver airport data, cleaned")
def silver_airport_data_clean():
  return (
    container.load_silver_data()
      .transform(container.filter_null_index_empty)
      .transform(container.add_processed_timestamp)
  )


@dlt.table(comment="Silver airport data, nulls")
def silver_airport_data_nulls():
  return (
    container.load_silver_data()
      .transform(container.filter_null_index_not_empty)
      .transform(container.add_processed_timestamp)
  )


@dlt.table(comment="Gold airports by country")
def gold_airports_by_country():
  return (
    container.load_silver_data_clean()
      .transform(container.agg_count_by_country)
      .transform(container.add_processed_timestamp)
  )  


# COMMAND ----------

from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType
from pyspark.sql.functions import when, col, array, array_union, lit, size, current_timestamp
import pandas as pd


url = "https://raw.githubusercontent.com/jpatokal/openflights/master/data/airports-extended.dat"


def load_data_file():  
  schema = StructType([
    StructField("airport_id", StringType(), True),
    StructField("name", StringType(), True),
    StructField("city", StringType(), True),
    StructField("country", StringType(), True),
    StructField("iata", StringType(), True),
    StructField("icao", StringType(), True),
    StructField("latitude", StringType(), True),
    StructField("longitude", StringType(), True),
    StructField("altitude", StringType(), True),
    StructField("timezone", StringType(), True),
    StructField("dst", StringType(), True),
    StructField("tz", StringType(), True),
    StructField("type", StringType(), True),
    StructField("source", StringType(), True)
  ])
  return spark.createDataFrame(pd.read_csv(url, header=None), schema)


def add_ingest_columns(df):
  return (
    df
      .withColumn("ingest_timestamp", container.timestamp_provider())
      .withColumn("ingest_source", lit(url.split("/")[-1]))
  )


def add_processed_timestamp(df):
  return df.withColumn("processed_timestamp", container.timestamp_provider())


def add_null_index_array(df):
  df = df.replace("\\N", None)
  df = df.withColumn("nulls", array())
  for i, c in enumerate(df.columns):
      df = df.withColumn("nulls", when(col(c).isNull(), array_union(col("nulls"), array(lit(i)))).otherwise(col("nulls")))
  return df.drop("ingest_timestamp", "ingest_source")


def filter_null_index_empty(df):
  return df.filter(size("nulls") == 0).drop("nulls")


def filter_null_index_not_empty(df):
  return df.filter(size("nulls") > 0)


def agg_count_by_country(df):
  return df.groupby("country").count()


def read_bronze_airport_data():
  return dlt.read("bronze_airport_data")


def read_silver_airport_data():
  return dlt.read("silver_airport_data")


def read_silver_airport_data_clean():
  return dlt.read("silver_airport_data_clean")


# COMMAND ----------

container.register(
  load_data_file = load_data_file,
  load_bronze_data = read_bronze_airport_data,
  load_silver_data = read_silver_airport_data,
  load_silver_data_clean = read_silver_airport_data_clean,
  add_ingest_columns = add_ingest_columns,
  add_processed_timestamp = add_processed_timestamp,
  add_null_index_array = add_null_index_array,
  filter_null_index_empty = filter_null_index_empty,
  filter_null_index_not_empty = filter_null_index_not_empty,
  agg_count_by_country = agg_count_by_country,
  timestamp_provider = current_timestamp
)
