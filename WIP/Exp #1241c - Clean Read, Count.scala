// Databricks notebook source
// MAGIC %md
// MAGIC # Experiment #1241
// MAGIC 
// MAGIC This notebook relies on datasets provided by Databricks Academy and mounted to **`/mnt/training-datasets`**.
// MAGIC 
// MAGIC For more information on mounting these datasets, please see <a target="_blank" href="https://www.databricks.training/step-by-step/mounting-training-datasets">Mounting Training Datasets</a><br/>
// MAGIC at <a target="_blank" href="https://www.databricks.training/step-by-step/mounting-training-datasets">https&#58;//www.databricks.training/step-by-step/mounting-training-datasets</a>.

// COMMAND ----------

// Location of the training datasets - adjust if necissary
val mountPoint = "/mnt/training"

// Verify that the training datasets are mounted
dbutils.fs.ls(mountPoint)
displayHTML("Mount is present and listable.")

// COMMAND ----------

val sourceDir = "dbfs:/mnt/training/global-sales/solutions/1990-to-2009.parquet"
display( dbutils.fs.ls(sourceDir) )

// COMMAND ----------

val schema = "transacted_at timestamp, trx_id integer, retailer_id integer, description string, amount decimal(38,2), city_id integer, new_at timestamp"
spark.read.schema(schema).parquet(sourceDir).count()