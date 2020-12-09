// Databricks notebook source
import org.apache.spark.sql.functions._

spark.sql("create database if not exists jdp")
spark.sql("use jdp")

val table = "transactions_par"
val targetDir = s"dbfs:/mnt/jacob-work/global-sales/solutions/2011-to-2018-100gb-par.delta" 

// COMMAND ----------

val maxPartitionBytes = (1342177280 * 10L) + "b"
spark.conf.set("spark.sql.files.maxPartitionBytes", maxPartitionBytes)
spark.conf.set("spark.databricks.delta.optimizeWrite.enabled", false)
spark.conf.set("spark.databricks.delta.autoCompact.enabled", false)

spark.sql(s"DROP TABLE IF EXISTS $table");
dbutils.fs.rm(targetDir, true)

spark.read.table("transactions")
  .withColumn("p_transacted_year", year($"transacted_at"))
  .repartition(12) // by month
  .write
  .partitionBy("p_transacted_year")
  .format("delta").save(targetDir)

// COMMAND ----------

spark.sql(s"CREATE TABLE IF NOT EXISTS $table USING DELTA LOCATION '$targetDir'");
spark.sql(s"VACUUM $table");