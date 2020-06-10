// Databricks notebook source
val df = spark.read.format("csv")
  .option("header", "true")
  .option("inferSchema", "true")
  .load("/FileStore/tables/insurance.csv")

display (df)

// COMMAND ----------

df.groupBy("sex").count().show()

// COMMAND ----------

// MAGIC %sql
// MAGIC select sex, count (*) from insurance where smoker='yes' group by sex

// COMMAND ----------

// MAGIC %sql
// MAGIC select region, sum(charges)total from insurance group by region order by 1 desc 
