# Databricks notebook source
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType
from pyspark.sql.functions import when, col, array, array_union, lit, size, current_timestamp

import dlt
import pandas as pd


@dlt.table(comment="Bronze airports data")
def bronze_airport_data():
  url = "https://raw.githubusercontent.com/jpatokal/openflights/master/data/airports-extended.dat"
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
  return (
    spark
      .createDataFrame(pd.read_csv(url, header=None), schema)
      .withColumn("ingest_timestamp", current_timestamp())
      .withColumn("ingest_source", lit(url.split("/")[-1]))
  )


@dlt.view(comment="Silver airport data, nulls identified")
def silver_airport_data():
  df = dlt.read("bronze_airport_data")
  df = df.replace("\\N", None)
  df = df.withColumn("nulls", array())
  for i, c in enumerate(df.columns):
      df = df.withColumn("nulls", when(col(c).isNull(), array_union(col("nulls"), array(lit(i)))).otherwise(col("nulls")))
  return df.drop("ingest_timestamp", "ingest_source")


@dlt.table(comment="Silver airport data, cleaned")
def silver_airport_data_clean():
  return dlt.read("silver_airport_data").filter(size("nulls") == 0).drop("nulls").withColumn("processed_timestamp", current_timestamp())


@dlt.table(comment="Silver airport data, nulls")
def silver_airport_data_nulls():
  return dlt.read("silver_airport_data").filter(size("nulls") > 0).withColumn("processed_timestamp", current_timestamp())


@dlt.table(comment="Gold airports by country")
def gold_airports_by_country():
  return dlt.read("silver_airport_data_clean").groupby("country").count().withColumn("processed_timestamp", current_timestamp())

