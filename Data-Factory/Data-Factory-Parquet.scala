// Databricks notebook source
import org.apache.spark.sql.functions._

spark.sql("create database if not exists jdp")
spark.sql("use jdp")

val table = "transactions_parquet"
val targetDir = s"dbfs:/mnt/training-rw/global-sales/solutions/2011-to-2018-100gb.parquet" 

// COMMAND ----------

spark.conf.set("spark.sql.files.maxPartitionBytes", "2g")
spark.conf.set("spark.databricks.delta.optimizeWrite.enabled", false)
spark.conf.set("spark.databricks.delta.autoCompact.enabled", false)

spark.sql(s"DROP TABLE IF EXISTS $table");
dbutils.fs.rm(targetDir, true)

spark
  .read.format("delta")
  .load(s"dbfs:/mnt/training-rw/global-sales/solutions/2011-to-2018-100gb.delta")
  .write.parquet(targetDir)

// COMMAND ----------

spark.sql(s"CREATE TABLE IF NOT EXISTS $table USING PARQUET LOCATION '$targetDir'");