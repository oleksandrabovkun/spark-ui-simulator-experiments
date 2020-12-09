// Databricks notebook source
import org.apache.spark.sql.functions._

spark.conf.set("spark.databricks.delta.optimizeWrite.enabled", false)
spark.conf.set("spark.databricks.delta.autoCompact.enabled", false)
spark.conf.set("spark.databricks.io.cache.enabled", true)

spark.sql("create database if not exists dbacademy")
spark.sql("use dbacademy")

val mount = "work-jacob"

val trxReadDir = s"dbfs:/mnt/$mount/global-sales/transactions/2011-to-2018-1tb.delta" 
val ctyReadDir = s"dbfs:/mnt/training/global-sales/cities/all.delta" 

// COMMAND ----------

val sizeGB = 1024                      // GB on S3
val buckets = sizeGB * 4               // On ingest, 128MB partitions

val trxTableName = s"transactions_1t_bucketed_$buckets"
val trxWriteDir =    s"dbfs:/mnt/$mount/global-sales/transactions/2011-to-2018-1tb-bkt_trx_$buckets.parquet" 

// COMMAND ----------

spark.sql(s"DROP TABLE IF EXISTS $trxTableName");
val trxDeleted = dbutils.fs.rm(trxWriteDir, true)

// COMMAND ----------

spark.read.format("delta").load(trxReadDir)
  .withColumnRenamed("city_id", "b_city_id")
  .repartition(buckets, $"b_city_id")
  .write
  .bucketBy(buckets, "b_city_id")
  .sortBy("b_city_id")
  .option("path", trxWriteDir)
  .saveAsTable(trxTableName)

// COMMAND ----------

val ctyTableName = s"cities_bucketed_$buckets"
val ctyWriteDir =  s"dbfs:/mnt/$mount/global-sales/cities/all_bkt-city-$buckets.parquet" 

spark.sql(s"DROP TABLE IF EXISTS $ctyTableName");
val ctyDeleted = dbutils.fs.rm(ctyWriteDir, true)

// COMMAND ----------

spark.read.format("delta").load(ctyReadDir)
  .withColumnRenamed("city_id", "b_city_id")
  .coalesce(1)
  .write
  .bucketBy(buckets, "b_city_id")
  .sortBy("b_city_id")
  .option("path", ctyWriteDir)
  .saveAsTable(ctyTableName)

// COMMAND ----------

display( spark.catalog.listTables.where($"name" contains "bucket") )

// COMMAND ----------

spark.sql(s"describe extended $trxTableName").show(25, false)

// COMMAND ----------

spark.sql(s"describe extended $ctyTableName").show(25, false)

// COMMAND ----------

spark.sessionState.catalog.getTableMetadata(org.apache.spark.sql.catalyst.TableIdentifier(trxTableName)).bucketSpec.foreach(println)

// COMMAND ----------

spark.sessionState.catalog.getTableMetadata(org.apache.spark.sql.catalyst.TableIdentifier(ctyTableName)).bucketSpec.foreach(println)

// COMMAND ----------

val newTrxTableName = trxTableName+"_test"

spark.sql(s"drop table if exists $newTrxTableName")

spark.sql(s"""
  CREATE TABLE $newTrxTableName(transacted_at timestamp, trx_id string, retailer_id integer, description string, amount decimal(38,2), b_city_id integer)
  USING parquet 
  CLUSTERED BY(b_city_id) INTO $buckets BUCKETS
  OPTIONS(PATH '$trxWriteDir')
""")

// COMMAND ----------

val newCtyTableName = ctyTableName+"_test"

spark.sql(s"drop table if exists $newCtyTableName")

spark.sql(s"""
  CREATE TABLE $newCtyTableName(b_city_id INT, city STRING, state STRING, state_abv STRING, country STRING)
  USING parquet 
  CLUSTERED BY(b_city_id) INTO $buckets BUCKETS
  OPTIONS(PATH '$ctyWriteDir')
""")

// COMMAND ----------

spark.sessionState.catalog.getTableMetadata(org.apache.spark.sql.catalyst.TableIdentifier(newTrxTableName)).bucketSpec.foreach(println)

// COMMAND ----------

spark.sessionState.catalog.getTableMetadata(org.apache.spark.sql.catalyst.TableIdentifier(newCtyTableName)).bucketSpec.foreach(println)

// COMMAND ----------

display( spark.read.table(newTrxTableName) )

// COMMAND ----------

display( spark.read.table(newCtyTableName) )

// COMMAND ----------

spark.conf.set("spark.sql.shuffle.partitions", buckets)

// COMMAND ----------

val trxNewReadDir = trxWriteDir.replace("training-rw", "training")
val trxDF = spark.read.parquet(trxNewReadDir)

val ctyNewReadDir = ctyWriteDir.replace("training-rw", "training")
val ctyDF = spark.read.parquet(ctyNewReadDir)

trxDF
  .join(ctyDF, "b_city_id")
  .write.mode("overwrite").format("noop").save()

// COMMAND ----------

val trxDF = spark.read.table(trxTableName)
val ctyDF = spark.read.table(ctyTableName)

spark.conf.set("spark.sql.autoBroadcastJoinThreshold", -1)

trxDF
  .join(ctyDF, "b_city_id")
  .write.mode("overwrite").format("noop").save()

// COMMAND ----------

val trxDF = spark.read.parquet(trxReadDir)
val ctyDF = spark.read.parquet(ctyReadDir)

trxDF
  .join(ctyDF, "city_id")
  .write.mode("overwrite").format("noop").save()

// COMMAND ----------

