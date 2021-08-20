# Databricks notebook source
# MAGIC %pip install unittest-xml-reporting
# MAGIC %pip install coverage

# COMMAND ----------

# MAGIC %run ./sample_tests

# COMMAND ----------

# MAGIC %run ./workflow_tests

# COMMAND ----------

# MAGIC %run ./dlt_workflow_refactored_unit_tests

# COMMAND ----------

# MAGIC %run ./dlt_workflow_refactored_integration_tests

# COMMAND ----------

import coverage.collector
import coverage
import coverage.python


should_trace = coverage.Coverage._should_trace
check_include_omit_etc = coverage.Coverage._check_include_omit_etc
get_python_source = coverage.python.get_python_source
reset = coverage.collector.Collector.reset

# COMMAND ----------

from typing import overload


def _find_code(name, lineno, names):
  def find_code(cells):
    if not lineno:
      return False
    cell_contents = cells[1]
    if not all(list(map(lambda x: cell_contents.find(str(x)), names))):
      return False
    lines = cell_contents.split("\n")
    if len(lines) < lineno:
      return False
    if name == "<module>":
      return lines[lineno-1].find(names[0]) != -1
    else:
      found = lines[lineno-1].strip().startswith(f"def {name}(")
      if not found:
        if lines[lineno-1].strip().startswith(f"@"):
          found = lines[lineno].strip().startswith(f"def {name}(")
      return found
  return find_code

cell_contents_map = {}
definitions_map = {}

def _should_trace(self, filename, frame):
    result = should_trace(self, filename=filename, frame=frame)
    if filename.startswith("<command-") and not frame.f_code.co_name == "<module>":
        result.trace = True
        result.reason = ""
        result.source_filename = filename
        code = list(filter(
          _find_code(
            frame.f_code.co_name, 
            frame.f_code.co_firstlineno, 
            frame.f_code.co_names
          ), 
          enumerate(globals()["In"])
        ))
        if code:
          cell_contents_map[filename] = code[0][0]          
          if code[0][1].split("\n")[frame.f_code.co_firstlineno-1].strip().startswith("@"):
            lines = set([frame.f_code.co_firstlineno, frame.f_code.co_firstlineno+1])
          else:
            lines = set([frame.f_code.co_firstlineno])
          filename = f"/databricks/driver/{filename}"
          if not definitions_map.get(filename):
            definitions_map[filename] = lines
          else:
            definitions_map[filename] |= lines
    elif (
      filename.startswith("/databricks") or 
      filename.startswith("/usr/local/lib/python3.8") or
      filename.startswith("/local_disk0/")
    ):
        result.trace = False
        result.reason = "Databricks library"
    return result


def _check_include_omit_etc(self, filename, frame):
    return check_include_omit_etc(self, filename=filename, frame=frame)
  
  
def _get_python_source(filename):
  if filename.startswith("/databricks/driver/<command-"):
    filename = filename.split("/")[-1]
    index = cell_contents_map.get(filename)
    if index is not None:
      return globals()["In"][index]
  return get_python_source(filename)


class protected_should_trace_cache(dict):
  def __init__(self, *args, **kwargs):    
    pass
  
  def __setitem__(self, key, value):
    pass

  def __getitem__(self, key):    
    return None
  
  def __delitem__(self, key):
    pass
  
  def __len__(self):
    return 0
  
  def __iter__(self):
    return {}
    
  def get(self, key, default=None):
    return None
  
  
def _reset(self):
  reset(self)
  self.should_trace_cache = protected_should_trace_cache()
  

setattr(coverage.Coverage, "_should_trace", _should_trace)
setattr(coverage.Coverage, "_check_include_omit_etc", _check_include_omit_etc)
setattr(coverage.collector.Collector, "reset", _reset)
coverage.python.get_python_source = _get_python_source

ctracer = coverage.collector.CTracer
coverage.collector.CTracer = None

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

def run_tests():  
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
  
  return out

# COMMAND ----------

def fixup_coverage_lines(c):  # pragma: no cover
  for k, v in cell_contents_map.items():
    lines = globals()["In"][v].split("\n")
    for i, line in enumerate(lines):
      if (
        line.strip().startswith("class") or
        line.strip().startswith("from") or
        line.strip().startswith("import")
      ):
        filename = f"/databricks/driver/{k}"
        if not definitions_map.get(filename):
          definitions_map[filename] = set([i+1])
        else:
          definitions_map[filename] |= set([i+1])
  cov._data.add_lines(definitions_map)

cov = coverage.Coverage()  # pragma: no cover
cov.start()                # pragma: no cover
test_output = run_tests()  # pragma: no cover
fixup_coverage_lines(cov)  # pragma: no cover
cov.stop()                 # pragma: no cover

# COMMAND ----------

test_output.seek(0)
test_results = ET.XML(test_output.read().decode('utf-8'))

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

from coverage.xmlreport import XmlReporter
import io


with io.StringIO() as out:
  report = XmlReporter(cov)
  report.report(None, out)
  out.seek(0)
  xml = out.read()

# COMMAND ----------

from coverage.jsonreport import JsonReporter
import io


with io.StringIO() as out:
  report = JsonReporter(cov)
  report.report(None, out)
  out.seek(0)
  json = out.read()
  
print(json)

# COMMAND ----------

setattr(coverage.Coverage, "_should_trace", should_trace)
setattr(coverage.Coverage, "_check_include_omit_etc", check_include_omit_etc)
setattr(coverage.collector.Collector, "reset", reset)
coverage.python.get_python_source = get_python_source

# COMMAND ----------

import pandas as pd

pdf = pd.read_json(json)

# COMMAND ----------

metadata = pdf.loc[["version", "timestamp", "branch_coverage", "show_contexts"], ["meta"]].T
metadata.index = [""]
metadata.T

# COMMAND ----------

statistics = pdf.loc[["num_statements", "covered_lines", "missing_lines", "excluded_lines", "percent_covered"], ["totals"]].T
statistics.index = ["Results"]
statistics = statistics.T
statistics["Results"] = statistics["Results"].apply(lambda x: int(round(x, 0)))
statistics.columns = [""]
statistics

# COMMAND ----------

from json import loads

files = pdf.drop(["version", "timestamp", "branch_coverage", "show_contexts", "covered_lines", "num_statements", "percent_covered", "missing_lines", "excluded_lines"]).drop(["meta", "totals"], axis=1)
files["file"] = files.index
files = files[
  files["file"].str[-1] == ">"
]
files.index = range(len(files.index))
files["executed_lines"] = files["files"].apply(lambda x: x.get("executed_lines"))
files["missing_lines"] = files["files"].apply(lambda x: x.get("missing_lines"))
files.loc[:, ["file", "executed_lines", "missing_lines"]]

# COMMAND ----------

from json import loads

files = pdf.drop(["version", "timestamp", "branch_coverage", "show_contexts", "covered_lines", "num_statements", "percent_covered", "missing_lines", "excluded_lines"]).drop(["meta", "totals"], axis=1)
files = files[
  files.index.str[-1] == ">"
]
files["file"] = files.index
files["executed_lines"] = files["files"].apply(lambda x: x.get("executed_lines"))
files["missing_lines"] = files["files"].apply(lambda x: x.get("missing_lines"))
files["code"] = files["file"].apply(lambda x: globals()["In"][cell_contents_map.get(x)])

html = "<div style=\"width:850px;height:1100px;overflow-x:auto;overflow-y:auto;padding:20px;\"><table><thead><tr><th></th><th></th></tr></thead><tbody>"

for row in files.itertuples():
  filename = row.file.replace("<", "").replace(">", "").replace("-", "/")
  label = row.file.replace("<", "&lt;").replace(">", "&gt;")
  html += f"<tr><td colspan=\"2\" style=\"background-color:#eee;font-weight:bold;font-size:11pt;\">{label}</td></tr>"
  
  code = row.code.split("\n")  
  for i, line in enumerate(code):
    style = ""
    if (i+1) in row.executed_lines:
      style = "background-color:#cfc;"
    elif (i+1) in row.missing_lines:
      style = "background-color:#fcc;"
    html += f"<tr><td style=\"{style}\"><pre>{i+1}</pre></td><td style=\"{style}\"><pre>{line}</pre></td></tr>"
    
  html += "<tr><td colspan=\"2\"><pre>&nbsp;</pre></td></tr>"

html += "</tbody></table></div>"

displayHTML(html)

# COMMAND ----------

test_output.seek(0)
dbutils.notebook.exit(test_output.read().decode('utf-8'))
