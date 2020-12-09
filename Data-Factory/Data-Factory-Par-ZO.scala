// Databricks notebook source
import org.apache.spark.sql.functions._

spark.sql("create database if not exists jdp")
spark.sql("use jdp")

val table = "transactions_par_zo"
val targetDir = s"dbfs:/mnt/training-rw/global-sales/solutions/2011-to-2018-100gb-par_year-zo_city.delta" 

// COMMAND ----------

val maxPartitionBytes = (1342177280 * 10L) + "b"
spark.conf.set("spark.sql.files.maxPartitionBytes", maxPartitionBytes)
spark.conf.set("spark.databricks.delta.optimizeWrite.enabled", false)
spark.conf.set("spark.databricks.delta.autoCompact.enabled", false)

spark.sql(s"DROP TABLE IF EXISTS $table");
dbutils.fs.rm(targetDir, true)

spark.read.table("transactions")
  .withColumn("p_transacted_year", year($"transacted_at"))
  .withColumnRenamed("city_id", "z_city_id")
  .withColumnRenamed("retailer_id", "z_retailer_id")
  .repartition(12) // by month
  .write
  .partitionBy("p_transacted_year")
  .format("delta").save(targetDir)

// COMMAND ----------

spark.sql(s"DROP TABLE IF EXISTS $table");
spark.sql(s"CREATE TABLE IF NOT EXISTS $table USING DELTA LOCATION '$targetDir'");
// spark.sql(s"OPTIMIZE $table ZORDER BY (z_city_id, z_retailer_id)")
spark.sql(s"VACUUM $table");