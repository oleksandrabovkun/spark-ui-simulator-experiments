// Databricks notebook source
import org.apache.spark.sql.functions._

spark.conf.set("spark.sql.files.maxPartitionBytes", "128m")
spark.conf.set("spark.databricks.delta.optimizeWrite.enabled", false)
spark.conf.set("spark.databricks.delta.autoCompact.enabled", false)

val mount = "work-jacob"

spark.sql("create database if not exists dbacademy")
spark.sql("use dbacademy")

val table = "transactions_1tb"
val trxReadPath = s"dbfs:/mnt/training/global-sales/transactions/2011-to-2018-100gb.delta"

val trxWriteDel = s"dbfs:/mnt/$mount/global-sales/transactions/2011-to-2018-1tb.delta" 
val trxWriteZo =  s"dbfs:/mnt/$mount/global-sales/transactions/2011-to-2018-1tb-zo_trx.delta"

// COMMAND ----------

spark.sql(s"DROP TABLE IF EXISTS $table");
val removedA = dbutils.fs.rm(trxWriteZo, true)
val removedB = dbutils.fs.rm(trxWriteDel, true)

// COMMAND ----------

spark.catalog.clearCache()
spark.conf.set("spark.sql.shuffle.partitions", 1)

val cityIds = spark.read.format("delta").load("dbfs:/mnt/training/global-sales/cities/all.delta").filter($"state".isNull).select("city_id").distinct.as[Int].collect
val cityZipsDF = cityIds.view.zipWithIndex.toDF("new_city_id", "index").coalesce(1).cache
val cityCount = cityZipsDF.count

display(cityZipsDF)

// COMMAND ----------

val partitions = 1024
val fileCount = dbutils.fs.ls(trxReadPath).filter(_.path.endsWith("parquet")).length
val iterations = (partitions / fileCount)-2

spark.conf.set("spark.sql.shuffle.partitions", partitions)

// COMMAND ----------

var df = spark
  .read.format("delta").load(trxReadPath)
  .withColumn("trx_id", concat($"trx_id", lit("-00")))

for (i <- 1 to iterations) {
  df = df.union(spark
    .read.format("delta").load(trxReadPath)
    .withColumn("trx_id", concat($"trx_id", lit(f"-$i%02d"))))
}
df.withColumn("index", (rand()*lit(cityCount)).cast("int"))
  .join(broadcast(cityZipsDF), "index")
  .withColumn("city_id", $"new_city_id")
  .drop("index", "new_city_id")
  .repartition(partitions)
  .select("transacted_at", "city_id", "trx_id", "retailer_id", "description", "amount")
  .write.format("delta").save(trxWriteDel)

// COMMAND ----------

// DBTITLE 1,Create Z-Ordered Version
spark
  .read.format("delta").load(trxWriteDel) // Load the table we just created
  .withColumnRenamed("trx_id", "z_trx_id")
  .withColumnRenamed("city_id", "z_city_id")
  .withColumnRenamed("retailer_id", "z_retailer_id")
  .repartition(partitions)
  .write.format("delta").save(trxWriteZo) // Make a copy of it

// COMMAND ----------

spark.sql(s"CREATE TABLE IF NOT EXISTS $table USING DELTA LOCATION '$trxWriteZo'");
spark.sql(s"OPTIMIZE $table ZORDER BY (z_trx_id, z_city_id, z_retailer_id)")
spark.sql(s"VACUUM $table");

// COMMAND ----------

// MAGIC %md
// MAGIC # Simple Read

// COMMAND ----------

spark.read.format("delta").load(trxWriteDel)
     .write.mode("overwrite").format("noop").save()

// COMMAND ----------

spark.read.format("delta").load(trxWriteZo)
     .write.mode("overwrite").format("noop").save()

// COMMAND ----------

// MAGIC %md
// MAGIC # z_trx_id

// COMMAND ----------

spark.read.format("delta").load(trxWriteDel)
     .filter($"trx_id" === "1018106826-03-00")
     .write.mode("overwrite").format("noop").save()

// COMMAND ----------

spark.read.format("delta").load(trxWriteZo)
     .filter($"z_trx_id" === "1018106826-03-00")
     .write.mode("overwrite").format("noop").save()

// COMMAND ----------

// MAGIC %md
// MAGIC # z_city_id

// COMMAND ----------

spark.read.format("delta").load(trxWriteDel)
     .filter($"city_id" === "654232306")
     .write.mode("overwrite").format("noop").save()


// COMMAND ----------

spark.read.format("delta").load(trxWriteZo)
     .filter($"z_city_id" === "654232306")
     .write.mode("overwrite").format("noop").save()

// COMMAND ----------

// MAGIC %md
// MAGIC # z_retailer_id

// COMMAND ----------

spark.read.format("delta").load(trxWriteDel)
     .filter($"retailer_id" === "847200066")
     .write.mode("overwrite").format("noop").save()

// COMMAND ----------

spark.read.format("delta").load(trxWriteZo)
     .filter($"z_retailer_id" === "847200066")
     .write.mode("overwrite").format("noop").save()


// COMMAND ----------

// MAGIC %md
// MAGIC # amount

// COMMAND ----------

spark.read.format("delta").load(trxWriteDel)
     .filter(trim($"amount") === 339.36)
     .write.mode("overwrite").format("noop").save()

// COMMAND ----------

spark.read.format("delta").load(trxWriteZo)
     .filter(trim($"amount") === 339.36)
     .write.mode("overwrite").format("noop").save()