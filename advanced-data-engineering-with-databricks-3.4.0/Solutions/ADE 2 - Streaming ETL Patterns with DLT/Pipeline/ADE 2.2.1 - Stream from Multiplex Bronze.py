# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning">
# MAGIC </div>
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC
# MAGIC # Stream from Multiplex Bronze
# MAGIC
# MAGIC Let's configure a query to consume and parse raw data from a single topic as it lands in the multiplex bronze table configured in the last lesson. We'll continue refining this query in the following notebooks.
# MAGIC
# MAGIC ## Learning Objectives
# MAGIC By the end of this lesson, you should be able to:
# MAGIC - Describe how filters are applied to streaming jobs
# MAGIC - Use built-in functions to flatten nested JSON data
# MAGIC - Parse and save binary-encoded strings to native types

# COMMAND ----------

import dlt
import pyspark.sql.functions as F

source = spark.conf.get("source")

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Parse Heart Rate Recordings
# MAGIC Define logic to parse our **`bpm`** topic to the following schema.
# MAGIC
# MAGIC | field | type |
# MAGIC | --- | --- |
# MAGIC | device_id | LONG | 
# MAGIC | time | TIMESTAMP | 
# MAGIC | heartrate | DOUBLE |

# COMMAND ----------

bpm_schema = "device_id LONG, time TIMESTAMP, heartrate DOUBLE"

@dlt.table(
    table_properties={"quality": "bronze"}
)
def bpm_bronze():
    return (
        dlt.read_stream("bronze")
          .filter("topic = 'bpm'")
          .select(F.from_json(F.col("value").cast("string"), bpm_schema).alias("v"))
          .select("v.*")
    )

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC &copy; 2024 Databricks, Inc. All rights reserved.<br/>
# MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the 
# MAGIC <a href="https://www.apache.org/">Apache Software Foundation</a>.<br/>
# MAGIC <br/><a href="https://databricks.com/privacy-policy">Privacy Policy</a> | 
# MAGIC <a href="https://databricks.com/terms-of-use">Terms of Use</a> | 
# MAGIC <a href="https://help.databricks.com/">Support</a>
