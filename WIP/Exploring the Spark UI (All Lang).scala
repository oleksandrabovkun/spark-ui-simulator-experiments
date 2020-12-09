// Databricks notebook source
// MAGIC %md
// MAGIC # Exploring the Spark UI

// COMMAND ----------

// MAGIC %run "/Curriculum/Modules/Common-Notebooks/Includes/Common-Notebooks/Dataset-Mounts"

// COMMAND ----------

// MAGIC %scala
// MAGIC 
// MAGIC val initialDF = spark                                                       
// MAGIC   .read                                                                     
// MAGIC   .parquet("/mnt/training/wikipedia/pagecounts/staging_parquet_en_only_clean/")   
// MAGIC   .cache()
// MAGIC 
// MAGIC initialDF.foreach(x => ()) // Processes each and every record
// MAGIC 
// MAGIC displayHTML("All done<br/><br/>")

// COMMAND ----------

// MAGIC %python
// MAGIC 
// MAGIC initialDF = (spark                                                       
// MAGIC   .read                                                                     
// MAGIC   .parquet("/mnt/training/wikipedia/pagecounts/staging_parquet_en_only_clean/")   
// MAGIC   .cache()
// MAGIC )
// MAGIC initialDF.foreach(lambda x: None) # Processes each and every record
// MAGIC 
// MAGIC displayHTML("All done<br/><br/>")

// COMMAND ----------

// MAGIC %sql
// MAGIC 
// MAGIC drop table if exists PageCounts;
// MAGIC 
// MAGIC create table PageCounts
// MAGIC using parquet
// MAGIC location 'dbfs:/mnt/training/wikipedia/pagecounts/staging_parquet_en_only_clean/';
// MAGIC 
// MAGIC cache table PageCounts

// COMMAND ----------

// MAGIC %md
// MAGIC ##![Spark Logo Tiny](https://files.training.databricks.com/images/105/logo_spark_tiny.png) Round #1 Questions
// MAGIC 0. How many jobs were triggered?
// MAGIC 0. Open the Spark UI and select the **Jobs** tab.
// MAGIC   0. What action triggered the first job?
// MAGIC   0. What action triggered the second job?
// MAGIC 0. Open the details for the second job, how many MB of data was read in? Hint: Look at the **Input** column.
// MAGIC 0. Open the details for the first stage of the second job, how many records were read in? Hint: Look at the **Input Size / Records** column.

// COMMAND ----------

// MAGIC %scala
// MAGIC 
// MAGIC import org.apache.spark.sql.functions.upper
// MAGIC 
// MAGIC val someDF = initialDF
// MAGIC   .withColumn("first", upper($"article".substr(0,1)) )
// MAGIC   .where( $"first".isin("A","B","C","D","E","F","G","H","I","J","K","L","M","N","O","P","Q","R","S","T","U","V","W","X","Y","Z") )
// MAGIC   .groupBy($"project", $"first").sum()
// MAGIC   .drop("sum(bytes_served)")
// MAGIC   .orderBy($"first", $"project")
// MAGIC   .select($"first", $"project", $"sum(requests)".as("total"))
// MAGIC   .filter($"total" > 10000)
// MAGIC 
// MAGIC val total = someDF.count().toInt
// MAGIC 
// MAGIC displayHTML("All done<br/><br/>")

// COMMAND ----------

// MAGIC %python
// MAGIC 
// MAGIC from pyspark.sql.functions import col, upper
// MAGIC 
// MAGIC someDF = (initialDF
// MAGIC   .withColumn("first", upper( col("article").substr(0,1)) )
// MAGIC   .where( col("first").isin("A","B","C","D","E","F","G","H","I","J","K","L","M","N","O","P","Q","R","S","T","U","V","W","X","Y","Z") )
// MAGIC   .groupBy("project", "first").sum()
// MAGIC   .drop("sum(bytes_served)")
// MAGIC   .orderBy("first", "project")
// MAGIC   .select( col("first"), col("project"), col("sum(requests)").alias("total"))
// MAGIC   .filter( col("total") > 10000)
// MAGIC )
// MAGIC total = someDF.count()
// MAGIC 
// MAGIC displayHTML("All done<br/><br/>")

// COMMAND ----------

// MAGIC %sql
// MAGIC 
// MAGIC create or replace temporary view RequestsByLetter as
// MAGIC   select first_letter,
// MAGIC          project,
// MAGIC          sum(requests) as total
// MAGIC   from (
// MAGIC      select *,
// MAGIC             substr(upper(article), 0, 1) as first_letter
// MAGIC      from PageCounts
// MAGIC   )
// MAGIC   where first_letter in ("A","B","C","D","E","F","G","H","I","J","K","L","M","N","O","P","Q","R","S","T","U","V","W","X","Y","Z")
// MAGIC   group by first_letter, project
// MAGIC   having total > 10000
// MAGIC   order by first_letter, project

// COMMAND ----------

// MAGIC %sql
// MAGIC 
// MAGIC select count(*) from RequestsByLetter

// COMMAND ----------

// MAGIC %md
// MAGIC ##![Spark Logo Tiny](https://files.training.databricks.com/images/105/logo_spark_tiny.png) Round #2 Questions
// MAGIC 0. How many jobs were triggered?
// MAGIC 0. How many actions were executed?
// MAGIC 0. Open the **Spark UI** and select the **Jobs** tab.
// MAGIC   0. What action triggered the first job?
// MAGIC   0. What action triggered the second job?
// MAGIC 0. Open the **SQL** tab - what is the relationship between these two jobs?
// MAGIC 0. For the first job...
// MAGIC   0. How many stages are there?
// MAGIC   0. Open the **DAG Visualization**. What do you suppose the green dot refers to?
// MAGIC 0. For the second job...
// MAGIC   0. How many stages are there?
// MAGIC   0. Open the **DAG Visualization**. Why do you suppose the first stage is grey?
// MAGIC   0. Can you figure out what transformation is triggering the shuffle at the end of 
// MAGIC     0. The first stage? Hint: If you can't figure it out, look at the SQL tab again.  Exchange means shuffle.  What happened after the shuffle?
// MAGIC     0. The second stage?
// MAGIC     0. The third stage? HINT: It's not a transformation but an action.
// MAGIC 0. For the second job, the second stage, how many records (total) 
// MAGIC   0. Were read in as a result of the previous shuffle operation?
// MAGIC   0. Were written out as a result of this shuffle operation?  
// MAGIC   Hint: look for the **Aggregated Metrics by Executor**
// MAGIC 0. Open the **Event Timeline** for the second stage of the second job.
// MAGIC   * Make sure to turn on all metrics under **Show Additional Metrics**.
// MAGIC   * Note that there were 200 tasks executed.
// MAGIC   * Visually compare the **Scheduler Delay** to the **Executor Computing Time**
// MAGIC   * Then in the **Summary Metrics**, compare the median **Scheduler Delay** to the median **Duration** (aka **Executor Computing Time**)
// MAGIC   * What is taking longer? scheduling, execution, task deserialization, garbage collection, result serialization or getting the result?

// COMMAND ----------

// MAGIC %scala
// MAGIC 
// MAGIC someDF.take(total)
// MAGIC 
// MAGIC displayHTML("All done<br/><br/>")

// COMMAND ----------

// MAGIC %python
// MAGIC 
// MAGIC someDF.take(total)
// MAGIC 
// MAGIC displayHTML("All done<br/><br/>")

// COMMAND ----------

// MAGIC %sql
// MAGIC 
// MAGIC select * from RequestsByLetter

// COMMAND ----------

// MAGIC %md
// MAGIC ##![Spark Logo Tiny](https://files.training.databricks.com/images/105/logo_spark_tiny.png) Round #3 Questions
// MAGIC 0. Collectively, `someDF.count()` produced 2 jobs and 6 stages.  
// MAGIC However, `someDF.take(total)` produced only 1 job and 2 stages.  
// MAGIC   0. Why did it only produce 1 job?
// MAGIC   0. Why did the last job only produce 2 stages?
// MAGIC 0. Look at the **Storage** tab. How many partitions were cached?
// MAGIC 0. True or False: The cached data is fairly evenly distributed.
// MAGIC 0. How many MB of data is being used by our cache?
// MAGIC 0. How many total MB of data is available for caching?
// MAGIC 0. Go to the **Executors** tab. How many executors do you have?
// MAGIC 0. Go to the **Executors** tab. How many total cores do you have available?
// MAGIC 0. Go to the **Executors** tab. What is the IP Address of your first executor?
// MAGIC 0. How many tasks is your cluster able to execute simultaneously?
// MAGIC 0. What is the path to your **Java Home**?

// COMMAND ----------

// MAGIC %md 
// MAGIC ## ![Spark Logo Tiny](https://files.training.databricks.com/images/105/logo_spark_tiny.png) Classroom-Cleanup<br>
// MAGIC 
// MAGIC Run the **`Classroom-Cleanup`** cell below to remove any artifacts created by this lesson.

// COMMAND ----------

// MAGIC %run "../Includes/Classroom-Cleanup"