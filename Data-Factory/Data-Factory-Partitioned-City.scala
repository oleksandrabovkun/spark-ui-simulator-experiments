// Databricks notebook source
import org.apache.spark.sql.functions._

spark.sql("create database if not exists jdp")
spark.sql("use jdp")

val table = "transactions_par_city"
val targetDir = s"dbfs:/mnt/jacob-work/global-sales/solutions/2011-to-2018-100gb-par_city.delta" 

// COMMAND ----------

val cityCount = spark
  .read.format("delta")
  .load("dbfs:/mnt/training/global-sales/solutions/2011-to-2018-100gb.delta")
  .select("city_id")
  .distinct
  .count.toInt

// COMMAND ----------

val maxPartitionBytes = (1342177280 * 10L) + "b"
spark.conf.set("spark.sql.files.maxPartitionBytes", maxPartitionBytes)
spark.conf.set("spark.databricks.delta.optimizeWrite.enabled", false)
spark.conf.set("spark.databricks.delta.autoCompact.enabled", false)

spark.sql(s"DROP TABLE IF EXISTS $table");
dbutils.fs.rm(targetDir, true)

val df = spark.read.format("delta").load("dbfs:/mnt/training/global-sales/solutions/2011-to-2018-100gb.delta")
  .withColumnRenamed("city_id", "p_city_id")
  .repartition(cityCount, $"p_city_id")
  .write
  .partitionBy("p_city_id")
  .format("delta").save(targetDir)


// COMMAND ----------

spark.sql(s"CREATE TABLE IF NOT EXISTS $table USING DELTA LOCATION '$targetDir'");
spark.sql(s"VACUUM $table");