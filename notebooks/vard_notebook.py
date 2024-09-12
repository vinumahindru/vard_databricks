# Databricks notebook source
# MAGIC %sql
# MAGIC select * from samples.nyctaxi.trips limit 10
# MAGIC ;
# MAGIC

# COMMAND ----------

from pyspark.sql.functions import *

df = spark.read.table("samples.nyctaxi.trips").orderBy("fare_amount", ascending=False).filter("fare_amount > 0")

df.limit(10).display()

# COMMAND ----------

dbutils.

# COMMAND ----------

# MAGIC %sh
# MAGIC
# MAGIC
