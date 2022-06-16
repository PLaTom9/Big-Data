// Databricks notebook source
import org.apache.spark.sql.functions._

val actorsPath = "dbfs:/FileStore/tables/Files/actors.csv"
spark.sql(s"CREATE DATABASE  IF NOT EXISTS movieDb")
val dfActors = spark.read.option("header","true").option("inferSchema","true").format("csv").load(actorsPath)
dfActors.write.mode("overwrite").saveAsTable(s"movieDb.actors")

// COMMAND ----------

display(dfActors)

// COMMAND ----------

dfActors.write.format("parquet").mode("overwrite").partitionBy("category").saveAsTable("ActorsAfterPartitioning")

// COMMAND ----------

// MAGIC %fs 
// MAGIC ls  /user/hive/warehouse/actorsafterpartitioning

// COMMAND ----------

dfActors.write.format("parquet").mode("overwrite").bucketBy(5,"category").saveAsTable("ActorsAfterBucket")

// COMMAND ----------

// MAGIC %fs 
// MAGIC ls  /user/hive/warehouse/actorsafterbucket

// COMMAND ----------

// MAGIC %sql
// MAGIC ANALYZE TABLE ActorsAfterBucket COMPUTE STATISTICS FOR ALL COLUMNS

// COMMAND ----------

// MAGIC %sql
// MAGIC DESCRIBE EXTENDED ActorsAfterBucket;

// COMMAND ----------

// MAGIC %sql
// MAGIC ANALYZE TABLE ActorsAfterPartitioning COMPUTE STATISTICS FOR ALL COLUMNS

// COMMAND ----------

// MAGIC %sql
// MAGIC DESCRIBE EXTENDED ActorsAfterPartitioning;
