# Databricks notebook source
from types import ModuleType
import sys

dlt = ModuleType("dlt")
sys.modules["dlt"] = dlt

def dlt_table(*args, **kwargs):
  def wrapper(*a):
      return a[0] if a else (args[0]() if args else None)
  return wrapper

def dlt_view(*args, **kwargs):
  def wrapper(*a):
      return a[0] if a else (args[0]() if args else None)
  return wrapper

def dlt_read(x):
  return None

dlt.table = dlt_table
dlt.view = dlt_view
dlt.read = dlt_read
