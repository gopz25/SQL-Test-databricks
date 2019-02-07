-- Databricks notebook source
DROP TABLE IF EXISTS diamonds;

CREATE TABLE diamonds USING CSV OPTIONS (path "/databricks-datasets/Rdatasets/data-001/csv/ggplot2/diamonds.csv", header "true")

-- COMMAND ----------

-- MAGIC %python
-- MAGIC 
-- MAGIC diamonds = spark.read.csv("/databricks-datasets/Rdatasets/data-001/csv/ggplot2/diamonds.csv", header="true", inferSchema="true")
-- MAGIC diamonds.write.format("delta").save("/delta/diamonds")

-- COMMAND ----------

DROP TABLE IF EXISTS diamonds;

CREATE TABLE diamonds USING DELTA LOCATION '/delta/diamonds/'

-- COMMAND ----------

SELECT color, avg(price) AS price FROM diamonds GROUP BY color ORDER BY COLOR


-- COMMAND ----------

-- MAGIC %sh ls /databricks/init_scripts/

-- COMMAND ----------

-- MAGIC %sh 
-- MAGIC 
-- MAGIC find /databricks/ -name "cluster"

-- COMMAND ----------

-- MAGIC %py
-- MAGIC 
-- MAGIC dbutils.fs.mkdirs("dbfs:/databricks/gopitest/")
-- MAGIC 
-- MAGIC dbutils.fs.put("/databricks/gopitest/postgresql-install.sh","""
-- MAGIC #!/bin/bash
-- MAGIC wget --quiet -O /mnt/driver-daemon/jars/postgresql-42.2.2.jar http://central.maven.org/maven2/org/postgresql/postgresql/42.2.2/postgresql-42.2.2.jar
-- MAGIC wget --quiet -O /mnt/jars/driver-daemon/postgresql-42.2.2.jar http://central.maven.org/maven2/org/postgresql/postgresql/42.2.2/postgresql-42.2.2.jar""", True)

-- COMMAND ----------

-- MAGIC %py 
-- MAGIC 
-- MAGIC display(dbutils.fs.ls("dbfs:/databricks/gopitest/postgresql-install.sh"))

-- COMMAND ----------

-- MAGIC %sh
-- MAGIC 
-- MAGIC curl -n -X POST -H 'Content-Type: application/json' -d '{
-- MAGIC   "cluster_id": "1202-211320-brick1",
-- MAGIC   "num_workers": 1,
-- MAGIC   "spark_version": "2.4.x-scala2.11",
-- MAGIC   "node_type_id": "i3.2xlarge",
-- MAGIC   "cluster_log_conf": {
-- MAGIC     "dbfs" : {
-- MAGIC       "destination": "dbfs:/cluster-logs"
-- MAGIC     }
-- MAGIC   },
-- MAGIC   "init_scripts": [ {
-- MAGIC     "dbfs": {
-- MAGIC       "destination": "dbfs:/databricks/gopitest/postgresql-install.sh"
-- MAGIC     }
-- MAGIC   } ]
-- MAGIC }' https:///api/2.0/clusters/edit

-- COMMAND ----------

-- MAGIC %py dbutils.fs.mkdirs("dbfs:/databricks/init/")

-- COMMAND ----------

-- MAGIC %py display(dbutils.fs.ls("dbfs:/databricks/init/"))

-- COMMAND ----------

-- MAGIC %py 
-- MAGIC 
-- MAGIC dbutils.fs.put("dbfs:/databricks/init/my-echo.sh" ,"""
-- MAGIC #!/bin/bash
-- MAGIC 
-- MAGIC echo "hello" >> /hello.txt
-- MAGIC """, True)

-- COMMAND ----------

-- MAGIC %py dbutils.fs.mkdirs("dbfs:/databricks/init/")

-- COMMAND ----------

-- MAGIC %py display(dbutils.fs.ls("dbfs:/databricks/init/"))

-- COMMAND ----------

-- MAGIC %py clusterName = "QS"

-- COMMAND ----------

-- MAGIC %py 
-- MAGIC dbutils.fs.mkdirs("dbfs:/databricks/init/%s/"%clusterName)

-- COMMAND ----------

-- MAGIC %py 
-- MAGIC 
-- MAGIC dbutils.fs.put("/databricks/init/%s/postgresql-install.sh"%clusterName,"""
-- MAGIC #!/bin/bash
-- MAGIC wget --quiet -O /mnt/driver-daemon/jars/postgresql-42.2.2.jar http://central.maven.org/maven2/org/postgresql/postgresql/42.2.2/postgresql-42.2.2.jar
-- MAGIC wget --quiet -O /mnt/jars/driver-daemon/postgresql-42.2.2.jar http://central.maven.org/maven2/org/postgresql/postgresql/42.2.2/postgresql-42.2.2.jar""", True)

-- COMMAND ----------

-- MAGIC %py 
-- MAGIC dbutils.fs.mkdirs("dbfs:/databricks/init/%s/"%clusterName)

-- COMMAND ----------

-- MAGIC %py display(dbutils.fs.ls("dbfs:/databricks/init/QS"))

-- COMMAND ----------

-- MAGIC %py display(dbutils.fs.ls("dbfs:/databricks/init/"))

-- COMMAND ----------

-- MAGIC %py display(dbutils.fs.ls("dbfs:/databricks/init/output/QS/2019-02-07_14-31-18/"))

-- COMMAND ----------

-- MAGIC %py dbutils.fs.head("dbfs:/databricks/init/output/QS/2019-02-07_14-31-18/my-echo.sh_10.72.247.56.log")

-- COMMAND ----------

-- MAGIC %py display(dbutils.fs.ls("dbfs:/databricks/init/output/QS/2019-02-07_14-31-18/"))

-- COMMAND ----------

