// Databricks notebook source
val expTotal = 2687821474L
val trxDir = "dbfs:/mnt/training/global-sales/transactions"

// COMMAND ----------

val total = spark.read.format("parquet").load(s"$trxDir/2011-to-2018-100gb-bkt_city_400.parquet/").count
assert(total == expTotal)

// COMMAND ----------

val total = spark.read.format("delta").load(s"$trxDir/2011-to-2018-100gb-par_city.delta/").count
assert(total == expTotal)

// COMMAND ----------

val total = spark.read.format("delta").load(s"$trxDir/2011-to-2018-100gb-par_year-zo_city.delta/").count
assert(total == expTotal)

// COMMAND ----------

val total = spark.read.format("delta").load(s"$trxDir/2011-to-2018-100gb-par_year-zo_city.delta/").count
assert(total == expTotal)

// COMMAND ----------

val total = spark.read.format("delta").load(s"$trxDir/2011-to-2018-100gb-par_year.delta/").count
assert(total == expTotal)

// COMMAND ----------

val total = spark.read.format("delta").load(s"$trxDir/2011-to-2018-100gb-zo_city.delta/").count
assert(total == expTotal)

// COMMAND ----------

val total = spark.read.format("delta").load(s"$trxDir/2011-to-2018-100gb.delta/").count
assert(total == expTotal)

// COMMAND ----------

val total = spark.read.format("parquet").load(s"$trxDir/2011-to-2018-100gb.parquet/").count
assert(total == expTotal)

// COMMAND ----------

val total = spark
  .read
  .option("header",true)
  .option("inferSchema", false)
  .csv(s"$trxDir/2011-to-2018-100gb.csv/").count

assert(total == expTotal)