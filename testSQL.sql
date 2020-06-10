-- Databricks notebook source
-- MAGIC %fs ls

-- COMMAND ----------

-- MAGIC %fs ls dbfs:/FileStore/tables

-- COMMAND ----------

-- MAGIC %scala 
-- MAGIC val insurance = spark.read.format("csv")
-- MAGIC   .option("header", "true")
-- MAGIC   .option("inferSchema", "true")
-- MAGIC   .load("/FileStore/tables/insurance.csv")
-- MAGIC 
-- MAGIC display(insurance)

-- COMMAND ----------

select sex, count(*) from insurance group by 1

-- COMMAND ----------

-- MAGIC %sql
-- MAGIC -- mode "FAILFAST" will abort file parsing with a RuntimeException if any malformed lines are encountered
-- MAGIC CREATE TEMPORARY TABLE insurance2
-- MAGIC   USING csv
-- MAGIC   OPTIONS (path "/FileStore/tables/insurance.csv", header "true", mode "FAILFAST")

-- COMMAND ----------

select sex, count(*) from insurance2 group by 1
