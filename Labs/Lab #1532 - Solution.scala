// Databricks notebook source
sc.setJobDescription(s"Cmd 1: Basic Setup")
import org.apache.spark.sql.functions._

// Enable Adaptive Query Execution
spark.conf.set("spark.sql.adaptive.enabled", true)
spark.conf.set("spark.sql.adaptive.skewedJoin.enabled", true)
spark.conf.set("spark.sql.adaptive.localShuffleReader.enabled", true)    
spark.conf.set("spark.sql.adaptive.coalescePartitions.enabled", true)

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

sc.setJobDescription(s"Cmd 2: Create a temp path for transactions")

val username = com.databricks.logging.AttributionContext.current.tags.collect({ case (t, v) if t.name == "user" => v }).head
val newTrxPath = f"dbfs:/user/$username/lab-1532/transactions.delta"

dbutils.fs.rm(newTrxPath, true)

// COMMAND ----------

sc.setJobDescription("Cmd 3: Fix our over-partitioned transactions")

spark
  .read.parquet(getTransactionsDir())
  .drop("year", "month", "day", "hour")
  .repartition(1)
  .write
  .format("delta")
  .save(newTrxPath)

// COMMAND ----------

sc.setJobDescription("Cmd 4: Create Transaction's DataFrame")

val trxDF = spark
  .read.format("delta")
  .load(newTrxPath)
  .select("transacted_at", "city_id", "amount")
  .filter(month($"transacted_at") === 11)

trxDF.printSchema

// COMMAND ----------

sc.setJobDescription("Cmd 5: Create City's DataFrame")

val ctyDF = spark
  .read.format("delta")
  .load(getCitiesDir())
  .filter($"country" === "USA")
  .select("city_id")

ctyDF.printSchema

// COMMAND ----------

sc.setJobDescription("Cmd 6: Apply our various transformations")

val resultsDF = trxDF
  .join(ctyDF, "city_id")
  .groupBy(dayofyear($"transacted_at"))
  .sum("amount")
  .withColumnRenamed("dayofyear(transacted_at)", "Day of Year")
  .withColumnRenamed("sum(amount)", "Sum")

// COMMAND ----------

sc.setJobDescription("Cmd 7: Process results")

display(resultsDF)

// COMMAND ----------

