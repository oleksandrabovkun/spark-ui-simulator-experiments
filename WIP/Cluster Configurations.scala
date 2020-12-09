// Databricks notebook source
// MAGIC %md
// MAGIC #Presentation Mode
// MAGIC To enable presentation mode, open the JavaScript console and enter the following command:
// MAGIC 
// MAGIC `window.settings.enablePresentationMode = true`
// MAGIC 
// MAGIC Then from the **View** menu select **Present**

// COMMAND ----------

// MAGIC %md-sandbox
// MAGIC <div style="display:table; height:600px; width:100%"><div style="display:table-cell; vertical-align:middle">
// MAGIC   <div style="margin: 0 auto; text-align:center; font-size:6vw">
// MAGIC     <p>Before we can talk Clusters</p>
// MAGIC     <p style="font-size:x-small">&nbsp;</p>
// MAGIC     <p>We need to talk Personas</p>
// MAGIC   </div>
// MAGIC </div></div>  

// COMMAND ----------

// MAGIC %md-sandbox
// MAGIC <div style="display:table; height:600px; width:100%"><div style="display:table-cell; vertical-align:middle">
// MAGIC   <div style="margin: 0 auto; font-size:5vw">
// MAGIC     <div>Four Personas</div>
// MAGIC     <ul style="margin-left:1em">
// MAGIC       <li>Data Analyst</li>
// MAGIC       <li>Data Scientist</li>
// MAGIC       <li>Engineers</li>
// MAGIC       <li>And Everyone Else</li>
// MAGIC     </ul>
// MAGIC   </div>
// MAGIC </div></div>  

// COMMAND ----------

// MAGIC %md-sandbox
// MAGIC <div style="display:table; height:600px; width:100%"><div style="display:table-cell; vertical-align:middle">
// MAGIC   <div style="margin: 0 auto; font-size:5vw">
// MAGIC     <div>Data Analyst</div>
// MAGIC     <ul style="margin-left:1em">
// MAGIC       <li>Pull data, aggregate it, report it</li>
// MAGIC       <li>Should be working with clean data</li>
// MAGIC       <li>Highly repetitive queries</li>
// MAGIC       <li>Large gaps between queries</li>
// MAGIC       <li>Quick to "join" two datasets</li>
// MAGIC     </ul>
// MAGIC   </div>
// MAGIC </div></div>  

// COMMAND ----------

// MAGIC %md-sandbox
// MAGIC <div style="display:table; height:600px; width:100%"><div style="display:table-cell; vertical-align:middle">
// MAGIC   <div style="margin: 0 auto; font-size:5vw">
// MAGIC     <div>Data Scientist</div>
// MAGIC     <ul style="margin-left:1em">
// MAGIC       <li>Large overlap with Data Analyst</li>
// MAGIC       <li>Statistical perspective to data</li>
// MAGIC       <li>Highly repetitive queries [at times]</li>
// MAGIC       <li>Engage in data preparation [regularly]</li>
// MAGIC       <li>Train models w/ highly iterative jobs</li>
// MAGIC     </ul>
// MAGIC   </div>
// MAGIC </div></div>  

// COMMAND ----------

// MAGIC %md-sandbox
// MAGIC <div style="display:table; height:600px; width:100%"><div style="display:table-cell; vertical-align:middle">
// MAGIC   <div style="margin: 0 auto; font-size:5vw;">
// MAGIC     <div>Engineers</div>
// MAGIC     <ul style="margin-left:1em">
// MAGIC       <li>Producing end-to-end applications</li>
// MAGIC       <li>Strong concern for execution time</li>
// MAGIC       <li>Building data pipelines (dirty &rarr; clean)</li>
// MAGIC       <li>Productionizing ML Models and reports</li>
// MAGIC       <li>Non repetitive, scheduled queries
// MAGIC     </ul>
// MAGIC   </div>
// MAGIC </div></div>  

// COMMAND ----------

// MAGIC %md-sandbox
// MAGIC <div style="display:table; height:600px; width:100%"><div style="display:table-cell; vertical-align:middle">
// MAGIC   <div style="margin: 0 auto; text-align:center; font-size:6vw">
// MAGIC     <p>Pigeonholed?</p>
// MAGIC     <img src="https://files.training.databricks.com/images/pigeons.png">
// MAGIC   </div>
// MAGIC </div></div>  

// COMMAND ----------

// MAGIC %md-sandbox
// MAGIC <div style="display:table; height:600px; width:100%"><div style="display:table-cell; vertical-align:middle">
// MAGIC   <div style="margin: 0 auto; text-align:center; font-size:6vw">
// MAGIC     <p>In truth, we wear many hats</p>
// MAGIC     <img src="https://files.training.databricks.com/images/hats-on-a-shelf.png" style="max-height: 500px">
// MAGIC   </div>
// MAGIC </div></div>  

// COMMAND ----------

// MAGIC %md-sandbox
// MAGIC <div style="display:table; height:600px; width:100%"><div style="display:table-cell; vertical-align:middle">
// MAGIC   <div style="margin: 0 auto">
// MAGIC     <p style="font-size:4vw">Given what we know about our typical users<br/>and the following clusters, which cluster is...</p>
// MAGIC     <p>&nbsp;</p>
// MAGIC     <img src="https://files.training.databricks.com/images/cluster-scenarios.png">
// MAGIC 
// MAGIC     <p>:SIDENOTE: Each configuration is essentially the same, using 100GB of RAM and 200 cores.</p>
// MAGIC 
// MAGIC   </div>
// MAGIC </div></div>  

// COMMAND ----------

// MAGIC %md-sandbox
// MAGIC <div style="display:table; height:600px; width:100%"><div style="display:table-cell; vertical-align:middle">
// MAGIC   <div style="margin: 0 auto">
// MAGIC     <div style="font-size:4vw">...best/least suited for the typical analyst?<br/><br/></div>
// MAGIC     <p>&nbsp;</p>
// MAGIC     <img src="https://files.training.databricks.com/images/cluster-scenarios.png">
// MAGIC     <p>:SIDENOTE: Each configuration is essentially the same, using 100GB of RAM and 200 cores.</p>
// MAGIC   </div>
// MAGIC </div></div>  

// COMMAND ----------

// MAGIC %md-sandbox
// MAGIC <div style="display:table; height:600px; width:100%"><div style="display:table-cell; vertical-align:middle">
// MAGIC   <div style="margin: 0 auto">
// MAGIC     <div style="font-size:4vw">...best/least suited to train an ML model?<br/><br/></div>
// MAGIC     <p>&nbsp;</p>
// MAGIC     <img src="https://files.training.databricks.com/images/cluster-scenarios.png">
// MAGIC     <p>:SIDENOTE: Each configuration is essentially the same, using 100GB of RAM and 200 cores.</p>
// MAGIC   </div>
// MAGIC </div></div>  

// COMMAND ----------

// MAGIC %md-sandbox
// MAGIC <div style="display:table; height:600px; width:100%"><div style="display:table-cell; vertical-align:middle">
// MAGIC   <div style="margin: 0 auto">
// MAGIC     <div style="font-size:4vw">...best/least suited for a nightly job that<br/>contains exclusively narrow transformations?</div>
// MAGIC     <p>&nbsp;</p>
// MAGIC     <img src="https://files.training.databricks.com/images/cluster-scenarios.png">
// MAGIC     <p>:SIDENOTE: Each configuration is essentially the same, using 100GB of RAM and 200 cores.</p>
// MAGIC   </div>
// MAGIC </div></div>  

// COMMAND ----------

// MAGIC %md-sandbox
// MAGIC <div style="display:table; height:600px; width:100%"><div style="display:table-cell; vertical-align:middle">
// MAGIC   <div style="margin: 0 auto">
// MAGIC     <div style="font-size:4vw">...best/least suited for a nightly job that<br/>contains numerous wide transformations?</div>
// MAGIC     <p>&nbsp;</p>
// MAGIC     <img src="https://files.training.databricks.com/images/cluster-scenarios.png">
// MAGIC     <p>:SIDENOTE: Each configuration is essentially the same, using 100GB of RAM and 200 cores.</p>
// MAGIC   </div>
// MAGIC </div></div>  

// COMMAND ----------

// MAGIC %md-sandbox
// MAGIC <div style="display:table; height:600px; width:100%"><div style="display:table-cell; vertical-align:middle">
// MAGIC   <div style="margin: 0 auto">
// MAGIC     <div style="font-size:4vw">...the most/least stable in light of an executor failure?<br/><br/></div>
// MAGIC     <p>&nbsp;</p>
// MAGIC     <img src="https://files.training.databricks.com/images/cluster-scenarios.png">
// MAGIC     <p>:SIDENOTE: Each configuration is essentially the same, using 100GB of RAM and 200 cores.</p>
// MAGIC   </div>
// MAGIC </div></div>  

// COMMAND ----------

// MAGIC %md-sandbox
// MAGIC <div style="display:table; height:600px; width:100%"><div style="display:table-cell; vertical-align:middle">
// MAGIC   <div style="margin: 0 auto">
// MAGIC     <div style="font-size:4vw">...most/least likely to induce<br/>long garbage collection sweeps?</div>
// MAGIC     <p>&nbsp;</p>
// MAGIC     <img src="https://files.training.databricks.com/images/cluster-scenarios.png">
// MAGIC     <p>:SIDENOTE: Each configuration is essentially the same, using 100GB of RAM and 200 cores.</p>
// MAGIC   </div>
// MAGIC </div></div>  

// COMMAND ----------

// MAGIC %md-sandbox
// MAGIC <div style="display:table; height:600px; width:100%"><div style="display:table-cell; vertical-align:middle">
// MAGIC   <div style="margin: 0 auto">
// MAGIC     <div style="font-size:4vw">...is most/least likely to experience an OOM Error?<br/><br/></div>
// MAGIC     <p>&nbsp;</p>
// MAGIC     <img src="https://files.training.databricks.com/images/cluster-scenarios.png">
// MAGIC     <p>:SIDENOTE: Each configuration is essentially the same, using 100GB of RAM and 200 cores.</p>
// MAGIC   </div>
// MAGIC </div></div>  

// COMMAND ----------

// MAGIC %md-sandbox
// MAGIC <div style="display:table; height:600px; width:100%"><div style="display:table-cell; vertical-align:middle">
// MAGIC   <div style="margin: 0 auto">
// MAGIC     <div style="font-size:4vw">...is going to cost the least/most amount of money?<br/><br/></div>
// MAGIC     <p>&nbsp;</p>
// MAGIC     <img src="https://files.training.databricks.com/images/cluster-scenarios.png">
// MAGIC     <p>:SIDENOTE: Each configuration is essentially the same, using 100GB of RAM and 200 cores.</p>
// MAGIC     <p>{{#amazon}}<b>See: <a href="https://aws.amazon.com/ec2/pricing/on-demand/" target="_blank">Amazon EC2 Pricing</a></b>{{/amazon}}</p>
// MAGIC     <p>{{#azure}}<b>See: <a href="https://azure.microsoft.com/en-us/pricing/details/virtual-machines/linux/#d-series" target="_blank">Azure VM Pricing</a></b>{{/azure}}</p>
// MAGIC   </div>
// MAGIC </div></div>  

// COMMAND ----------

// MAGIC %md-sandbox
// MAGIC <div style="display:table; height:600px; width:100%"><div style="display:table-cell; vertical-align:middle">
// MAGIC   <div style="margin: 0 auto">
// MAGIC     <div style="font-size:4vw">In what cases does it make sense to<br/>put multiple executors on a single node?</div>
// MAGIC     <p>&nbsp;</p>
// MAGIC     <img src="https://files.training.databricks.com/images/cluster-scenarios.png">
// MAGIC     <p>:SIDENOTE: Each configuration is essentially the same, using 100GB of RAM and 200 cores.</p>
// MAGIC   </div>
// MAGIC </div></div>  