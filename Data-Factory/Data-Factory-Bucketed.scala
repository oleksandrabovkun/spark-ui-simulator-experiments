// Databricks notebook source
val awsAccessKey = dbutils.secrets.get(scope = "aws", key = "aws-access-key-files-rw")
val awsSecretKey = dbutils.secrets.get(scope = "aws", key = "aws-secret-key-files-rw").replace("/", "%2F")

val awsAuth = s"${awsAccessKey}:${awsSecretKey}"
val source = s"s3a://${awsAuth}@databricks-corp-training/common"

if (dbutils.fs.mounts.map(_.mountPoint).contains("/mnt/training-rw")){
  println("Already mounted")
} else {
  dbutils.fs.mount(source, "/mnt/training-rw")
}
println("-"*80)

// COMMAND ----------

import org.apache.spark.sql.functions._

spark.conf.set("spark.databricks.delta.optimizeWrite.enabled", false)
spark.conf.set("spark.databricks.delta.autoCompact.enabled", false)
spark.conf.set("spark.databricks.io.cache.enabled", true)

spark.sql("create database if not exists dbacademy")
spark.sql("use dbacademy")

// val buckets = 400
val buckets = 800

spark.conf.set("spark.sql.shuffle.partitions", buckets)

// val mount = "work-jacob"
val mount = "training-rw"

val trxTableName = s"transactions_bucketed_$buckets"
val ctyTableName = s"cities_bucketed_$buckets"

val trxWriteDir = s"dbfs:/mnt/$mount/global-sales/transactions/2011-to-2018-100gb-bkt_city_$buckets.parquet" 
val ctyWriteDir = s"dbfs:/mnt/$mount/global-sales/cities/all_bkt_city_$buckets.parquet" 

val trxReadDir = s"dbfs:/mnt/training/global-sales/transactions/2011-to-2018-100gb.parquet" 
val ctyReadDir = s"dbfs:/mnt/training/global-sales/cities/all.parquet" 

// COMMAND ----------

spark.sql(s"DROP TABLE IF EXISTS $trxTableName");
val trxDeleted = dbutils.fs.rm(trxWriteDir, true)

// COMMAND ----------

spark.read.parquet(trxReadDir)
  .withColumnRenamed("city_id", "b_city_id")
  .repartition(buckets, $"b_city_id")
  .write
  .bucketBy(buckets, "b_city_id")
  .sortBy("b_city_id")
  .option("path", trxWriteDir)
  .saveAsTable(trxTableName)

// COMMAND ----------

spark.sql(s"DROP TABLE IF EXISTS $ctyTableName");
val ctyDeleted = dbutils.fs.rm(ctyWriteDir, true)

// COMMAND ----------

spark.read.parquet(ctyReadDir)
  .withColumnRenamed("city_id", "b_city_id")
  .coalesce(1)
  .write
  .bucketBy(buckets, "b_city_id")
  .sortBy("b_city_id")
  .option("path", ctyWriteDir)
  .saveAsTable(ctyTableName)

// COMMAND ----------

val testTrxDF = spark.read.table(trxTableName)
val testCtyDF = spark.read.table(ctyTableName)

val testDF = testTrxDF.join(testCtyDF, "b_city_id")

testDF.write.format("noop").mode("overwrite").save()

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

dbutils.fs.unmount("/mnt/training-rw")

// COMMAND ----------

