# Databricks notebook source
# MAGIC %sql
# MAGIC select * from samples.nyctaxi.trips limit 10
# MAGIC ;
# MAGIC

# COMMAND ----------

from pyspark.sql.functions import *

df = spark.read.table("samples.nyctaxi.trips").orderBy("fare_amount", ascending=False).filter("fare_amount > 0")

df.limit(20).display()

# COMMAND ----------

# MAGIC %sh
# MAGIC ls -lrt
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select  *
# MAGIC from    samples.tpc.orders o
# MAGIC
