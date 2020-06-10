# Databricks notebook source
# Configure MLflow Experiment
mlflow_experiment_id = 866112

# Including MLflow
import mlflow
import mlflow.spark

import os
print("MLflow Version: %s" % mlflow.__version__)

# COMMAND ----------

# MAGIC %fs ls dbfs:/FileStore/tables

# COMMAND ----------

# MAGIC %python
# MAGIC 
# MAGIC fraud_data = spark.read.format('csv').options(header='true', inferSchema='true').load('/FileStore/tables/fraud.csv')
# MAGIC 
# MAGIC display(fraud_data)

# COMMAND ----------

fraud_data.printSchema()

# COMMAND ----------

# Calculate the differences between originating and destination balances
fraud_data = fraud_data.withColumn("orgDiff", fraud_data.newbalanceOrig - fraud_data.oldbalanceOrg).withColumn("destDiff", fraud_data.newbalanceDest - fraud_data.oldbalanceDest)

# Create temporary view
fraud_data.createOrReplaceTempView("financials")

# COMMAND ----------

display(fraud_data)

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Organize by Type
# MAGIC select type, count(1) from financials group by type

# COMMAND ----------

# MAGIC %sql
# MAGIC select type, sum(amount) from financials group by type

# COMMAND ----------

from pyspark.sql import functions as F

# Rules to Identify Known Fraud-based
fraud_data = fraud_data.withColumn("label", 
                   F.when(
                     (
                       (fraud_data.oldbalanceOrg <= 56900) & (fraud_data.type == "TRANSFER") & (fraud_data.newbalanceDest <= 105)) | 
                       (
                         (fraud_data.oldbalanceOrg > 56900) & (fraud_data.newbalanceOrig <= 12)) | 
                           (
                             (fraud_data.oldbalanceOrg > 56900) & (fraud_data.newbalanceOrig > 12) & (fraud_data.amount > 1160000)
                           ), 1
                   ).otherwise(0))

# Calculate proportions
fraud_cases = fraud_data.filter(fraud_data.label == 1).count()
total_cases = fraud_data.count()
fraud_pct = 1.*fraud_cases/total_cases

# Provide quick statistics
print("Based on these rules, we have flagged %s (%s) fraud cases out of a total of %s cases." % (fraud_cases, fraud_pct, total_cases))

# Create temporary view to review data
fraud_data.createOrReplaceTempView("financials_labeled")


# COMMAND ----------

# MAGIC %sql
# MAGIC select label, count(1) as `Transactions`, sum(amount) as `Total Amount` from financials_labeled group by label

# COMMAND ----------

# MAGIC %sql
# MAGIC -- where sum(destDiff) >= 10000000.00
# MAGIC select nameOrig, nameDest, label, TotalOrgDiff, TotalDestDiff
# MAGIC   from (
# MAGIC      select nameOrig, nameDest, label, sum(OrgDiff) as TotalOrgDiff, sum(destDiff) as TotalDestDiff 
# MAGIC        from financials_labeled 
# MAGIC       group by nameOrig, nameDest, label 
# MAGIC      ) a
# MAGIC  where TotalDestDiff >= 1000000
# MAGIC  limit 100

# COMMAND ----------

# MAGIC %sql
# MAGIC select type, label, count(1) as `Transactions` from financials_labeled group by type, label

# COMMAND ----------

# Initially split our dataset between training and test datasets
(train, test) = fraud_data.randomSplit([0.8, 0.2], seed=12345)

# Cache the training and test datasets
train.cache()
test.cache()

# Print out dataset counts
print("Total rows: %s, Training rows: %s, Test rows: %s" % (fraud_data.count(), train.count(), test.count()))

# COMMAND ----------

from pyspark.ml import Pipeline
from pyspark.ml.feature import StringIndexer
from pyspark.ml.feature import OneHotEncoderEstimator
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.classification import DecisionTreeClassifier

# Encodes a string column of labels to a column of label indices
indexer = StringIndexer(inputCol = "type", outputCol = "typeIndexed")

# VectorAssembler is a transformer that combines a given list of columns into a single vector column
va = VectorAssembler(inputCols = ["typeIndexed", "amount", "oldbalanceOrg", "newbalanceOrig", "oldbalanceDest", "newbalanceDest", "orgDiff", "destDiff"], outputCol = "features")

# Using the DecisionTree classifier model
dt = DecisionTreeClassifier(labelCol = "label", featuresCol = "features", seed = 54321, maxDepth = 5)

# Create our pipeline stages
pipeline = Pipeline(stages=[indexer, va, dt])

# COMMAND ----------

# View the Decision Tree model (prior to CrossValidator)
dt_model = pipeline.fit(train)
display(dt_model.stages[-1])
