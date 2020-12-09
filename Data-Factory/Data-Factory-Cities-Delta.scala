// Databricks notebook source
import org.apache.spark.sql.functions._
val ctyWriteDir = s"dbfs:/mnt/training-rw/global-sales/cities/all.delta" 
val ctyReadDir = s"dbfs:/mnt/training/global-sales/cities/all.delta" 

spark.conf.set("spark.sql.files.maxPartitionBytes", "1280m") // 1.25g
spark.conf.set("spark.databricks.delta.optimizeWrite.enabled", false)
spark.conf.set("spark.databricks.delta.autoCompact.enabled", false)
spark.conf.set("spark.databricks.io.cache.enabled", true)

// COMMAND ----------

dbutils.fs.rm(ctyWriteDir, true)

spark.read.format("parquet").load("dbfs:/mnt/training/global-sales/cities/all.parquet")
  .repartition(1)
  .write.format("delta")
  .save(ctyWriteDir)

// COMMAND ----------

val df = spark
  .read.format("delta")
  .load(ctyReadDir)

display(df)