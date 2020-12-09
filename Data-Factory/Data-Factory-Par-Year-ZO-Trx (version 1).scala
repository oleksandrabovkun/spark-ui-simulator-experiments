// Databricks notebook source
import org.apache.spark.sql.functions._

spark.sql("create database if not exists jdp")
spark.sql("use jdp")

val table = "transactions_par_year_zo_trx_v1"
val targetDir = s"dbfs:/mnt/jacob-work/global-sales/transactions/2011-to-2018-100gb-par_year-zo_trx.delta.v1" 

spark.sql(s"DROP TABLE IF EXISTS $table");
dbutils.fs.rm(targetDir, true)

// COMMAND ----------

spark.conf.set("spark.sql.files.maxPartitionBytes", "1152m")
spark.conf.set("spark.databricks.delta.optimizeWrite.enabled", true)
spark.conf.set("spark.databricks.delta.autoCompact.enabled", false)

val years = 8        // 2011 to 2018
val partitions = 100 // I just know

val initialDF = spark
  .read.format("delta")
  .load("dbfs:/mnt/training/global-sales/transactions/2011-to-2018-100gb.delta")
  .withColumn("p_transacted_year", year($"transacted_at"))
  .withColumnRenamed("trx_id", "z_trx_id")
  .repartition(partitions/years)


// COMMAND ----------

initialDF
  .write
  .partitionBy("p_transacted_year")
  .format("delta").save(targetDir)

// COMMAND ----------

spark.sql(s"DROP TABLE IF EXISTS $table");
spark.sql(s"CREATE TABLE IF NOT EXISTS $table USING DELTA LOCATION '$targetDir'");
spark.sql(s"OPTIMIZE $table ZORDER BY (z_trx_id)")
spark.sql(s"VACUUM $table");