// Databricks notebook source
import org.apache.spark.sql.functions._

spark.sql("create database if not exists jdp")
spark.sql("use jdp")

val table = "transactions_par_year_zo_trx_v2"
val targetDir = s"dbfs:/mnt/jacob-work/global-sales/transactions/2011-to-2018-100gb-par_year-zo_trx.delta.v2" 

spark.sql(s"DROP TABLE IF EXISTS $table");
dbutils.fs.rm(targetDir, true)

// COMMAND ----------

spark.conf.set("spark.sql.files.maxPartitionBytes", "1152m") // 1024+128
spark.conf.set("spark.databricks.delta.optimizeWrite.enabled", true)
spark.conf.set("spark.databricks.delta.autoCompact.enabled", false)

val years = 8
val gigs = 100
val gigsPerYear = gigs/years

for (y <- 2011 to 2018) {
  println(f"Processing $y")
  val targetYear = s"$targetDir/p_transacted_year=$y"

  val initialDF = spark
    .read
    .load("dbfs:/mnt/training/global-sales/transactions/2011-to-2018-100gb.delta")
    .filter(year($"transacted_at") === y)
    .withColumnRenamed("trx_id", "z_trx_id")
    .repartition(gigsPerYear)
    .write
    .parquet(targetYear) // NOT DELTA !!!
}

spark.sql(f"CONVERT TO DELTA parquet.$targetDir) PARTITIONED BY (p_transacted_year)")


// COMMAND ----------

display( dbutils.fs.ls(targetDir) )

// COMMAND ----------

spark.sql(s"DROP TABLE IF EXISTS $table");
spark.sql(s"CREATE TABLE IF NOT EXISTS $table USING DELTA LOCATION '$targetDir'");
spark.sql(s"OPTIMIZE $table ZORDER BY (z_trx_id)")
spark.sql(s"VACUUM $table");