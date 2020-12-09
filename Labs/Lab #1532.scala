// Databricks notebook source
sc.setJobDescription("Cmd 1: Basic Setup")
import org.apache.spark.sql.functions._

def getTransactionsDir():String = {
  // Obfuscated so that the file path doesn't tip off the problem
  val encodedPath = "ZGJmczovbW50L3RyYWluaW5nL2dsb2JhbC1zYWxlcy90cmFuc2FjdGlvbnMvMjAxNS5wYXJxdWV0" 
  new String(java.util.Base64.getDecoder.decode(encodedPath))
}

def getCitiesDir():String = {
  // Obfuscated so that the file path doesn't tip off the problem
  val encodedPath = "ZGJmczovbW50L3RyYWluaW5nL2dsb2JhbC1zYWxlcy9jaXRpZXMvYWxsLmRlbHRh"
  new String(java.util.Base64.getDecoder.decode(encodedPath))
}

// COMMAND ----------

sc.setJobDescription("Cmd 2: Create Transaction's DataFrame")

// Note: The data-on-disk is partitioned by year, month, day and hour
val trxDF = spark.read.parquet(getTransactionsDir())

trxDF.printSchema

// COMMAND ----------

sc.setJobDescription("Cmd 3: Create City's DataFrame")

val ctyDF = spark.read.format("delta").load(getCitiesDir())

ctyDF.printSchema

// COMMAND ----------

sc.setJobDescription("Cmd 4: Apply our various transformations")

val resultsDF = trxDF
  .join(ctyDF, "city_id")
  .filter(month($"transacted_at") === 11 && $"country" === "USA")
  .groupBy(dayofyear($"transacted_at"))
  .sum("amount")
  .withColumnRenamed("dayofyear(transacted_at)", "Day of Year")
  .withColumnRenamed("sum(amount)", "Sum")

// COMMAND ----------

sc.setJobDescription("Cmd 5: Process results")

display(resultsDF)

// COMMAND ----------

