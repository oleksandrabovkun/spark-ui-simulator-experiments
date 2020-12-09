// Databricks notebook source
import org.apache.spark.sql.functions._

spark.sql("create database if not exists jdp")
spark.sql("use jdp")

val table = "transactions_csv"
val trxWriteDir = s"dbfs:/mnt/training-rw/global-sales/transactions/2011-to-2018-100gb.csv" 
val trxReadDir =  trxWriteDir.replace("training-rw", "training")

// COMMAND ----------

spark.conf.set("spark.sql.files.maxPartitionBytes", "1152m")
spark.conf.set("spark.databricks.delta.optimizeWrite.enabled", false)
spark.conf.set("spark.databricks.delta.autoCompact.enabled", false)

spark.sql(s"DROP TABLE IF EXISTS $table");
dbutils.fs.rm(trxWriteDir, true)

spark
  .read.format("delta")
  .load(s"dbfs:/mnt/training-rw/global-sales/transactions/2011-to-2018-100gb.delta")
  .repartition(200) // Cutting files size down by 1/2
  .write.option("header",true)
  .csv(trxWriteDir)

// COMMAND ----------

spark.sql(s"CREATE TABLE IF NOT EXISTS $table USING PARQUET LOCATION '$trxReadDir'");