# Databricks notebook source
data_path = "wasbs://spark-ui-simulator@dbacademy.blob.core.windows.net/global-sales/transactions/2011-to-2018-100gb-par_year.delta"
data_path_not_part = "wasbs://spark-ui-simulator@dbacademy.blob.core.windows.net/global-sales/transactions/2011-to-2018-100gb.delta"
target_city = 365900539

# COMMAND ----------

# MAGIC %md
# MAGIC <table>
# MAGIC   <tr>
# MAGIC     <td></td>
# MAGIC       <td>VM</td>
# MAGIC       <td>Quantity</td>
# MAGIC       <td>Total Cores</td>
# MAGIC       <td>Total RAM</td>
# MAGIC   </tr>
# MAGIC   <tr>
# MAGIC       <td>Driver:</td>
# MAGIC       <td>**Standard_DS12_v2/i3.xlarge**</td>
# MAGIC       <td>**1**</td>
# MAGIC       <td>**4 cores**</td>
# MAGIC       <td>**28.0/30.5 GB**</td>
# MAGIC   </tr>
# MAGIC   <tr>
# MAGIC       <td>Workers:</td>
# MAGIC       <td>**Standard_DS12_v2/i3.xlarge**</td>
# MAGIC       <td>**4**</td>
# MAGIC       <td>**16 cores**</td>
# MAGIC       <td>**112/122 GB**</td>
# MAGIC   </tr>
# MAGIC </table>

# COMMAND ----------

sc.setJobDescription("Step A: Basic initialization")
from pyspark.sql.functions import *
spark.conf.set("spark.databricks.io.cache.enabled", "false")

# COMMAND ----------

sc.setJobDescription("Step B: No predicates")
# No filter applied
spark.read.format("delta").load(data_path_not_part)\
  .write.format("noop").mode("overwrite").save()

# COMMAND ----------

sc.setJobDescription("Step C: Predicate filter")
# Not optimized for any type of filtering
spark.read\
  .format("delta").load(data_path_not_part)\
  .where(col("city_id") == target_city)\
  .where(year(col("transacted_at")) == 2013)\
  .write.format("noop").mode("overwrite").save()

# COMMAND ----------

sc.setJobDescription("Step D: Partition filter")
# Dataset is partitioned by transaction year
spark.read\
  .format("delta").load(data_path_part)\
  .where(col("city_id") == target_city)\
  .where(col("p_transacted_year") == 2013)\
  .write.format("noop").mode("overwrite").save()

# COMMAND ----------

sc.setJobDescription("Step E: Filter propogation")
# Filter can be pushed down even through shuffle operation
spark.read\
  .format("delta").load(data_path_part)\
  .groupBy("city_id").agg(sum(col("amount")))\
  .where(col("city_id") == target_city)\
  .write.format("noop").mode("overwrite").save()

# COMMAND ----------

sc.setJobDescription("Step F: Partition filter not applied")
# Although dataset is partitoined, no utilization of partitioning
spark.read\
  .format("delta").load(data_path_part)\
  .where(col('city_id') == target_city)\
  .where(year(col("transacted_at")) == 2013)\
  .write.format("noop").mode("overwrite").save()

# COMMAND ----------

sc.setJobDescription("Step G: Filter on sum column")
# Filter on aggregate column is not split into filters on compoments columns
spark.read\
  .format("delta").load(data_path_part)\
  .withColumn("tax", col("amount")*0.2)\
  .withColumn("total",  col("amount") + col("tax"))\
  .where(col("total") > 400)\
  .write.format("noop").mode("overwrite").save()

# COMMAND ----------

sc.setJobDescription("Step H: Crippling the Predicate")
# Extra cache in between prevents filters pushdown
spark.read\
  .format("delta").load(data_path_part)\
  .cache()\
  .where(col("city_id") == target_city)\
  .where(col("p_transacted_year") == 2013)\
  .write.format("noop").mode("overwrite").save()
