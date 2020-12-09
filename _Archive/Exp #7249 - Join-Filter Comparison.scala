// Databricks notebook source
import org.apache.spark.sql.functions._

val maxPartitionMB = 256           // default (128mb)
val factor = 1024d/maxPartitionMB  // factor to adjust shuffle by
val maxPartitions = 100            // 100gb at 1gb each, ideally
val shufflePartitions = (maxPartitions*factor).toInt

spark.conf.set("spark.sql.files.maxPartitionBytes", maxPartitionMB+"m")
spark.conf.set("spark.sql.shuffle.partitions", shufflePartitions)
spark.conf.set("spark.databricks.io.cache.enabled", "false")

// COMMAND ----------

val citiesDF = spark
  .read.format("delta")
  .load("dbfs:/mnt/training/global-sales/cities/all.delta")
  .filter($"country" === "USA")

display( citiesDF )

// COMMAND ----------

// Parquet file (not optimized)
val parquetDF = spark
  .read.format("parquet")
  .load("dbfs:/mnt/training/global-sales/transactions/2011-to-2018-100gb.parquet")
  .hint("skew", "city_id")

parquetDF
  .join(citiesDF, "city_id")
  .foreach(_=>())

// COMMAND ----------

spark.conf.get("spark.sql.adaptive.enabled")

// COMMAND ----------

// Delta file (not optimized)
val deltaDF = spark
  .read.format("delta")
  .load("dbfs:/mnt/training/global-sales/transactions/2011-to-2018-100gb.delta")
  .hint("skew", "city_id")

deltaDF
  .join(citiesDF, "city_id")
  .foreach(_=>())

// COMMAND ----------

// Partitioned by city
val parDF = spark
  .read.format("delta")
  .load("dbfs:/mnt/training/global-sales/transactions/2011-to-2018-100gb-par_city.delta")
  .hint("skew", "p_city_id")

parDF
  .join(citiesDF, $"city_id" === $"p_city_id")
  .foreach(_=>())

// COMMAND ----------

// Z-Ordered by city
val zoDF = spark
  .read.format("delta")
  .load("dbfs:/mnt/training/global-sales/transactions/2011-to-2018-100gb-zo_city.delta")
  .hint("skew", "z_city_id")

zoDF
  .join(citiesDF, $"city_id" === $"z_city_id")
  .foreach(_=>())

// COMMAND ----------

// Configure the bucketing tables from existing datasets
val numBuckets = 100 // Predetermined
spark.sql(s"CREATE DATABASE IF NOT EXISTS dbacademy")
spark.sql(s"USE dbacademy")

// Setup the bucketed transactions table
val trxTable = "transactions_bucketed"
val trxBucketPath = "dbfs:/mnt/training/global-sales/transactions/2011-to-2018-100gb-bkt_city_400.parquet"
spark.sql(s"DROP TABLE IF EXISTS $trxTable")

spark.sql(s"""CREATE TABLE $trxTable(transacted_at timestamp, trx_id string, retailer_id integer, description string, amount decimal(38,2), b_city_id integer)
              USING parquet CLUSTERED BY(b_city_id) INTO $numBuckets BUCKETS OPTIONS(PATH '$trxBucketPath')""")

// Setup the bucketed cities table
val ctyTable = "cities_bucketed"
val ctyBucketPath = s"dbfs:/mnt/training/global-sales/cities/all_bkt_city_400.parquet" 
spark.sql(s"DROP TABLE IF EXISTS $ctyTable")

spark.sql(s"""CREATE TABLE $ctyTable(transacted_at timestamp, trx_id string, retailer_id integer, description string, amount decimal(38,2), b_city_id integer)
              USING parquet CLUSTERED BY(b_city_id) INTO $numBuckets BUCKETS OPTIONS(PATH '$ctyBucketPath')""")

// COMMAND ----------

// Bucketed by city

val ctyBkDF = spark.read.table(ctyTable)
val trxBkDF = spark.read.table(trxTable).hint("skew", "b_city_id")

trxBkDF.join(ctyBkDF, "b_city_id").foreach(_=>())

// COMMAND ----------

