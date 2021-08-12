# Databricks notebook source
import unittest

# COMMAND ----------

class SampleTests(unittest.TestCase):
  def tests_always_succeeds(self):
    self.assertTrue(True)
  
  def test_always_fails(self):
    self.assertTrue(False)
