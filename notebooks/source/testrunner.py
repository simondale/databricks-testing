# Databricks notebook source
# MAGIC %pip install unittest-xml-reporting

# COMMAND ----------

# MAGIC %run ./sample_tests

# COMMAND ----------

#%run ./workflow_tests

# COMMAND ----------

#%run ./dlt_workflow_refactored_unit_tests

# COMMAND ----------

#%run ./dlt_workflow_refactored_integration_tests

# COMMAND ----------

import xml.etree.ElementTree as ET
import pandas as pd
import unittest
import xmlrunner
import uuid
import io
import datetime
import json

# COMMAND ----------

from inspect import getmro

def get_class_hierarchy(t):
  try:
    return getmro(t)
  except:
    return [object]

test_classes = {t for t in globals().values() if unittest.case.TestCase in get_class_hierarchy(t) and t != unittest.case.TestCase}
print(test_classes)

loader = unittest.TestLoader()
suite = unittest.TestSuite()
for test_class in test_classes:
  tests = loader.loadTestsFromTestCase(test_class)
  suite.addTests(tests)

out = io.BytesIO()
runner = xmlrunner.XMLTestRunner(out)
runner.run(suite)

# COMMAND ----------

out.seek(0)
test_results = ET.XML(out.read().decode('utf-8'))

ts = []
for suite in test_results:
  for test in suite:
    failures = [{k:v for k,v in failure.items()} for failure in test]
    if len(failures) > 0:
      for failure in failures:
        attributes = {k:v for k,v in suite.attrib.items()}
        attributes.update({f"test_{k}":v for k,v in test.attrib.items()})
        attributes.update({f"failure_{k}":v for k,v in failure.items()})
        ts.append(attributes)
    else:
      attributes = {k:v for k,v in suite.attrib.items()}
      attributes.update({f"test_{k}":v for k,v in test.attrib.items()})
      attributes.update({"failure_type":None, "failure_message":None})
      ts.append(attributes)
    
df = pd.DataFrame(ts)
df["tests"] = df["tests"].astype(int)
df["errors"] = df["errors"].astype(int)
df["failures"] = df["failures"].astype(int)
df["skipped"] = df["skipped"].astype(int)
df["succeeded"] = df["tests"] - (df["errors"] + df["failures"] + df["skipped"])
df["name"] = df["name"].apply(lambda x: str.join("-", x.split("-")[:-1]))
df = df.loc[:, [
#   "timestamp", 
  "name", 
#   "time", 
  "tests", 
  "succeeded", 
  "errors", 
  "failures", 
  "skipped", 
  "test_name", 
  "test_time", 
  "failure_type", 
  "failure_message"
]]
df

# COMMAND ----------

idx = df.groupby(["name", "tests", "succeeded", "errors", "failures", "skipped"]).first().index

gf = pd.DataFrame([[x for x in t] for t in idx], columns=idx.names)
gf.index = gf["name"]
gf = gf.iloc[:,2:]
gf.T.plot.pie(subplots=True, colors=['green', 'orange', 'red', 'yellow'], labeldistance=None, figsize=(8,8), legend=None)

# COMMAND ----------

from matplotlib import pyplot as plt

plt.rcParams["figure.autolayout"] = True

group = df.groupby(["name"]).first()

group.loc[:, ["succeeded", "errors", "failures", "skipped"]].plot(kind="barh", stacked=True, color=['green', 'orange', 'red', 'yellow'], xticks=[], legend=None, xlabel="")


# COMMAND ----------

out.seek(0)
dbutils.notebook.exit(out.read().decode('utf-8'))
