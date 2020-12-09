// Databricks notebook source
val awsAccessKey = dbutils.secrets.get(scope = "aws", key = "aws-access-key-files-rw")
val awsSecretKey = dbutils.secrets.get(scope = "aws", key = "aws-secret-key-files-rw").replace("/", "%2F")

val awsAuth = s"${awsAccessKey}:${awsSecretKey}"
val source = s"s3a://${awsAuth}@databricks-corp-training/common"

dbutils.fs.mount(source, "/mnt/training-rw")

// COMMAND ----------

spark.conf.set("spark.sql.files.maxPartitionBytes", f"${1024+128}m")

// val mount = "work-jacob"
val mount = "training-rw"

val trxReadPath = "dbfs:/mnt/training/global-sales/transactions/2011-to-2018-100gb.parquet"
val trxSchema = "transacted_at TIMESTAMP, trx_id STRING, retailer_id INT, description STRING, amount DECIMAL(38,2), city_id INT"

val usaCount = 45
val intCount = 60

val trxUsaWritePath = f"dbfs:/mnt/$mount/global-sales/transactions/2011-to-2018_usa-only_${usaCount}gb.delta"
val trxIntWritePath = f"dbfs:/mnt/$mount/global-sales/transactions/2011-to-2018_int-only_${intCount}gb.delta"

// COMMAND ----------

val ctyReadPath = "dbfs:/mnt/training/global-sales/cities/all.parquet" 
val ctySchema = "city_id integer, city string, state string, state_abv string, country string"

val usCities = spark
  .read.schema(ctySchema).parquet(ctyReadPath)
  .filter($"state".isNotNull)
  .select($"city_id")
  .as[Integer]
  .collect

// COMMAND ----------

dbutils.fs.rm(trxUsaWritePath, true)

spark
  .read.schema(trxSchema).parquet(trxReadPath)
  .filter($"city_id".isin(usCities:_ *) === true)
  .repartition(usaCount)
  .write.mode("overwrite").format("delta").save(trxUsaWritePath)

// COMMAND ----------

val files = dbutils.fs.ls(trxUsaWritePath).filter(_.path.endsWith(".parquet"))

displayHTML(f"""<table>
<tr><td>Count:   </td><td>${files.length}%,d </td></tr>
<tr><td>Size:    </td><td>${files.map(_.size).sum}%,d </td></tr>
<tr><td>Average: </td><td>${files.map(_.size).sum/files.length/1024/1024}%,d </td></tr>
</table>""")

// COMMAND ----------

val trxUsaDF = spark
  .read.format("delta").load(trxUsaWritePath)
  .groupBy("city_id").count

display(trxUsaDF)

// COMMAND ----------

dbutils.fs.rm(trxIntWritePath, true)

spark
  .read.schema(trxSchema).parquet(trxReadPath)
  .filter($"city_id".isin(usCities:_ *) === false)
  .repartition(intCount)
  .write.mode("overwrite").format("delta").save(trxIntWritePath)

// COMMAND ----------

val files = dbutils.fs.ls(trxIntWritePath).filter(_.path.endsWith(".parquet"))

displayHTML(f"""<table>
<tr><td>Count:   </td><td>${files.length}%,d </td></tr>
<tr><td>Size:    </td><td>${files.map(_.size).sum}%,d </td></tr>
<tr><td>Average: </td><td>${files.map(_.size).sum/files.length/1024/1024}%,d </td></tr>
</table>""")

// COMMAND ----------

val trxIntDF = spark
  .read.format("delta").load(trxIntWritePath)
  .groupBy("city_id").count

display(trxIntDF)

// COMMAND ----------

println(trxUsaWritePath)
println(trxIntWritePath)

// COMMAND ----------

dbutils.fs.unmount("/mnt/training-rw")

// COMMAND ----------

val trxUsaPath = trxUsaWritePath.replace("training-rw", "training")
val usaCount = dbutils.fs.ls(trxUsaPath).filter(_.path.endsWith(".parquet")).length
assert(usaCount == usaCount)

val trxIntPath = trxIntWritePath.replace("training-rw", "training")
val intCount = dbutils.fs.ls(trxIntPath).filter(_.path.endsWith(".parquet")).length
assert(intCount == intCount)