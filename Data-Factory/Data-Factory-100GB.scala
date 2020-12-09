// Databricks notebook source
import org.apache.spark.sql.functions._
val maxPartitionBytes = (1342177280 * 4L) + "b"
spark.conf.set("spark.sql.files.maxPartitionBytes", maxPartitionBytes)

spark.conf.set("spark.databricks.delta.optimizeWrite.enabled", true)
spark.conf.set("spark.databricks.delta.autoCompact.enabled", true)

spark.sql("create database if not exists jdp")
spark.sql("use jdp")

val targetDir = "dbfs:/mnt/jacob-work/global-sales/solutions/2011-to-2018-temp.delta"

// COMMAND ----------

spark.sql(s"DROP TABLE IF EXISTS transactions");
dbutils.fs.rm(targetDir, true)

// COMMAND ----------

// MAGIC %md # Create DataFrames

// COMMAND ----------

// val trx2015DF = spark
//   .read
//   .schema("transacted_at timestamp, trx_id integer, retailer_id integer, description string, amount decimal(38,2), city_id integer, year integer, month integer, day integer, hour integer")
//   .parquet("dbfs:/mnt/training/global-sales/transactions/2015.parquet")

// display(trx2015DF)

// COMMAND ----------

// MAGIC %md # Write the files

// COMMAND ----------

spark
  .read
  .parquet("dbfs:/mnt/training/global-sales/transactions/2011.parquet")
  .select($"trx_id".cast("integer"), 
          $"city_id".cast("integer"),
          $"retailer_id".cast("integer"), 
          $"transacted_at".cast("timestamp"),
          $"description".cast("string"), 
          $"amount".cast("decimal(38,2)"))
  .withColumn("file", input_file_name())
  .write.format("delta").save(targetDir)

// COMMAND ----------

spark
  .read
  .option("inferSchema", false)
  .schema("transacted_at timestamp, trx_id integer, retailer_id integer, description string, amount decimal(38,2), city_id integer")
  .csv("dbfs:/mnt/training/global-sales/transactions/2012.csv")
  .withColumn("file", input_file_name())
  .write.mode("append").format("delta").save(targetDir)

// COMMAND ----------

spark
  .read
  .schema("transacted_at timestamp, trx_id integer, retailer_id integer, description string, amount decimal(38,2), city_id integer")
  .parquet("dbfs:/mnt/training/global-sales/transactions/2013.parquet")
  .withColumn("file", input_file_name())
  .write.mode("append").format("delta").save(targetDir)

// COMMAND ----------

spark
  .read
  .schema("transacted_at timestamp, trx_id integer, retailer_id integer, description string, amount decimal(38,2), city_id integer, year integer, month integer")
  .parquet("dbfs:/mnt/training/global-sales/transactions/2014.parquet")
  .drop("year", "month")
  .withColumn("file", input_file_name())
  .write.mode("append").format("delta").save(targetDir)

// COMMAND ----------

spark
  .read
  .schema("transacted_at timestamp, trx_id integer, retailer_id integer, description string, amount decimal(38,2), city_id integer")
  .parquet("dbfs:/mnt/training/global-sales/transactions/2015.parquet")
  .drop("year", "month", "day", "hour")
  .withColumn("file", input_file_name())
  .write.mode("append").format("delta").save(targetDir)

// COMMAND ----------

spark
  .read
  .schema("transacted_at timestamp, trx_id integer, retailer_id integer, description string, amount decimal(38,2), city_id integer")
  .parquet("dbfs:/mnt/training/global-sales/transactions/2016.parquet")
  .withColumn("file", input_file_name())
  .write.mode("append").format("delta").save(targetDir)

// COMMAND ----------

spark
  .read
  .schema("transacted_at timestamp, trx_id integer, retailer_id integer, description string, amount decimal(38,2), city_id integer")
  .parquet("dbfs:/mnt/training/global-sales/transactions/2017.parquet")
  .withColumn("file", input_file_name())
  .write.mode("append").format("delta").save(targetDir)

// COMMAND ----------

spark
  .read
  .schema("transacted_at timestamp, trx_id integer, retailer_id integer, description string, amount decimal(38,2), city_id integer, year integer, month integer")
  .parquet("dbfs:/mnt/training/global-sales/transactions/2018.parquet")
  .drop("year", "month")
  .withColumn("file", input_file_name())
  .write.mode("append").format("delta").save(targetDir)

// COMMAND ----------

// MAGIC %md # Inflate dataset

// COMMAND ----------

val finalDF = spark
  .read.format("delta").load(targetDir)
  .repartition(sc.defaultParallelism) // spread before exploding

  .withColumn("other-id", lit(Seq("01","02","03","04","05","06","07","08","09").toArray))
  .select($"transacted_at", $"trx_id", $"retailer_id", $"description", $"amount", $"city_id", explode($"other-id").as("other-id"))
  .withColumn("trx_id", concat($"trx_id", lit("-"), $"other-id"))
  .drop("other-id")
  
  .withColumn("min", (rand()*120).cast("int")-60)
  .withColumn("interval", concat(lit("interval "), $"min", lit(" minutes")  ).cast("interval"))
  .withColumn("transacted_at", $"transacted_at" + $"interval")
  .drop("min", "interval")

  .withColumn("amount", (rand() * 500).cast("decimal(38,2)"))

// COMMAND ----------

val finalDir = "dbfs:/mnt/jacob-work/global-sales/solutions/2011-to-2018-100gb.delta"
val deleted = dbutils.fs.rm(finalDir, true)
spark.sql(s"DROP TABLE IF EXISTS transactions");

// COMMAND ----------

spark.conf.set("spark.databricks.delta.optimizeWrite.enabled", false)
spark.conf.set("spark.databricks.delta.autoCompact.enabled", false)

finalDF
  .filter(year($"transacted_at") =!= 2010) // drop 2010 data 
  .repartition(100)                        // to control target file size
  .write.format("delta").save(finalDir)    // write as delta

// COMMAND ----------

val files = dbutils.fs.ls(finalDir).filter(_.name.endsWith(".parquet"))
val avgSize = (files.map(_.size).sum/files.size)/1024.0 / 1024.0 / 1024.0
val maxSize = (files.map(_.size).max)/1024.0 / 1024.0 / 1024.0
val minSize = (files.map(_.size).min)/1024.0 / 1024.0 / 1024.0
val sumSize = (files.map(_.size).sum)/1024.0 / 1024.0 / 1024.0

displayHTML(f"""
File Count: ${files.size}<br/>
Min Size: ${minSize}%.02f GB<br/>
Max Size: ${maxSize}%.02f GB<br/>
Avg Size: ${avgSize}%.02f GB<br/>
Sum Size: ${sumSize}%.02f GB<br/>
""")

// COMMAND ----------

spark.sql(s"CREATE TABLE IF NOT EXISTS transactions USING DELTA LOCATION '$finalDir'");
spark.sql(s"VACUUM transactions");

// COMMAND ----------

