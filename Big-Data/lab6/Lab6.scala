// Databricks notebook source
// MAGIC %sql
// MAGIC Create database if not exists Sample

// COMMAND ----------

spark.sql("create database if not exists Sample")

// COMMAND ----------

// MAGIC %sql
// MAGIC 
// MAGIC CREATE TABLE IF NOT EXISTS Sample.Transactions ( AccountId INT, TranDate DATE, TranAmt DECIMAL(8, 2));
// MAGIC 
// MAGIC CREATE TABLE IF NOT EXISTS Sample.Logical (RowID INT,FName VARCHAR(20), Salary SMALLINT);

// COMMAND ----------

// MAGIC %sql
// MAGIC 
// MAGIC INSERT INTO Sample.Transactions VALUES 
// MAGIC ( 1, '2011-01-01', 500),
// MAGIC ( 1, '2011-01-15', 50),
// MAGIC ( 1, '2011-01-22', 250),
// MAGIC ( 1, '2011-01-24', 75),
// MAGIC ( 1, '2011-01-26', 125),
// MAGIC ( 1, '2011-01-28', 175),
// MAGIC ( 2, '2011-01-01', 500),
// MAGIC ( 2, '2011-01-15', 50),
// MAGIC ( 2, '2011-01-22', 25),
// MAGIC ( 2, '2011-01-23', 125),
// MAGIC ( 2, '2011-01-26', 200),
// MAGIC ( 2, '2011-01-29', 250),
// MAGIC ( 3, '2011-01-01', 500),
// MAGIC ( 3, '2011-01-15', 50 ),
// MAGIC ( 3, '2011-01-22', 5000),
// MAGIC ( 3, '2011-01-25', 550),
// MAGIC ( 3, '2011-01-27', 95 ),
// MAGIC ( 3, '2011-01-30', 2500)

// COMMAND ----------

import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.types.{DoubleType, DateType,StringType, IntegerType,DecimalType, StructField, StructType}
import org.apache.spark.sql.functions._

val schema_transactions = new StructType()
  .add(StructField("AccountId", IntegerType, true))
  .add(StructField("TranDate", StringType, true))
  .add(StructField("TranAmt", DoubleType, true))

val data_transactions: RDD[Row] = sc.parallelize(
           Seq(Row( 1, "2011-01-01", 500.0),
        Row( 1, "2011-01-15", 50.0),
        Row( 1, "2011-01-22", 250.0),
        Row( 1, "2011-01-24", 75.0),
        Row( 1, "2011-01-26", 125.0),
        Row( 1, "2011-01-28", 175.0),
        Row( 2, "2011-01-01", 500.0),
        Row( 2, "2011-01-15", 50.0),
        Row( 2, "2011-01-22", 25.0),
        Row( 2, "2011-01-23", 125.0),
        Row( 2, "2011-01-26", 200.0),
        Row( 2, "2011-01-29", 250.0),
        Row( 3, "2011-01-01", 500.0),
        Row( 3, "2011-01-15", 50.0),
        Row( 3, "2011-01-22", 5000.0),
        Row( 3, "2011-01-25", 550.0),
        Row( 3, "2011-01-27", 950.0),
        Row( 3, "2011-01-30", 2500.0))
  )


val sample_transactions = spark.createDataFrame(data_transactions, schema_transactions).withColumn("TranDate", to_date(col("TranDate")).as("TranDate"))

// COMMAND ----------

// MAGIC %sql
// MAGIC INSERT INTO Sample.Logical
// MAGIC VALUES (1,'George', 800),
// MAGIC (2,'Sam', 950),
// MAGIC (3,'Diane', 1100),
// MAGIC (4,'Nicholas', 1250),
// MAGIC (5,'Samuel', 1250),
// MAGIC (6,'Patricia', 1300),
// MAGIC (7,'Brian', 1500),
// MAGIC (8,'Thomas', 1600),
// MAGIC (9,'Fran', 2450),
// MAGIC (10,'Debbie', 2850),
// MAGIC (11,'Mark', 2975),
// MAGIC (12,'James', 3000),
// MAGIC (13,'Cynthia', 3000),
// MAGIC (14,'Christopher', 5000);

// COMMAND ----------

val schema_logical = new StructType()
  .add(StructField("RowID", IntegerType, true))
  .add(StructField("FName", StringType, true))
  .add(StructField("Salary", IntegerType, true))

val data_logical: RDD[Row] = sc.parallelize(
           Seq(Row(1,"George", 800),
        Row(2,"Sam", 950),
        Row(3,"Diane", 1100),
        Row(4,"Nicholas", 1250),
        Row(5,"Samuel", 1250),
        Row(6,"Patricia", 1300),
        Row(7,"Brian", 1500),
        Row(8,"Thomas", 1600),
        Row(9,"Fran", 2450),
        Row(10,"Debbie", 2850),
        Row(11,"Mark", 2975),
        Row(12,"James", 3000),
        Row(13,"Cynthia", 3000),
        Row(14,"Christopher", 5000))
  )


val sample_logical = spark.createDataFrame(data_logical, schema_logical)

// COMMAND ----------

// MAGIC %md 
// MAGIC Totals based on previous row

// COMMAND ----------

// MAGIC %sql
// MAGIC 
// MAGIC SELECT AccountId,
// MAGIC TranDate,
// MAGIC TranAmt,
// MAGIC -- running total of all transactions
// MAGIC SUM(TranAmt) OVER (PARTITION BY AccountId ORDER BY TranDate) as RunTotalAmt
// MAGIC FROM Sample.Transactions ORDER BY AccountId, TranDate;

// COMMAND ----------

import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window

val window = Window.partitionBy("AccountId").orderBy("TranDate")
sample_transactions.withColumn("RunTotalAmt",sum("TranAmt").over(window)).orderBy("AccountId", "TranDate").show(true)

// COMMAND ----------

// MAGIC %sql
// MAGIC SELECT AccountId,
// MAGIC TranDate,
// MAGIC TranAmt,
// MAGIC -- running average of all transactions
// MAGIC AVG(TranAmt) OVER (PARTITION BY AccountId ORDER BY TranDate) as RunAvg,
// MAGIC -- running total # of transactions
// MAGIC COUNT(*) OVER (PARTITION BY AccountId ORDER BY TranDate) as RunTranQty,
// MAGIC -- smallest of the transactions so far
// MAGIC MIN(TranAmt) OVER (PARTITION BY AccountId ORDER BY TranDate) as RunSmallAmt,
// MAGIC -- largest of the transactions so far
// MAGIC MAX(TranAmt) OVER (PARTITION BY AccountId ORDER BY TranDate) as RunLargeAmt,
// MAGIC -- running total of all transactions
// MAGIC SUM(TranAmt) OVER (PARTITION BY AccountId ORDER BY TranDate) RunTotalAmt
// MAGIC FROM Sample.Transactions 
// MAGIC ORDER BY AccountId,TranDate;

// COMMAND ----------

val window = Window.partitionBy("AccountId").orderBy("TranDate")
sample_transactions
.withColumn("RunAvg",avg("TranAmt").over(window)).withColumn("RunTranQty",count("*").over(window))
.withColumn("RunSmallAmt",min("TranAmt").over(window))
.withColumn("RunLargeAmt",max("TranAmt").over(window))
.withColumn("RunTotalAmt",sum("TranAmt").over(window))
.orderBy("AccountId", "TranDate").show(true)

// COMMAND ----------

// MAGIC %md 
// MAGIC * Calculating Totals Based Upon a Subset of Rows

// COMMAND ----------

// MAGIC %sql
// MAGIC SELECT AccountId,
// MAGIC TranDate,
// MAGIC TranAmt,
// MAGIC -- average of the current and previous 2 transactions
// MAGIC AVG(TranAmt) OVER (PARTITION BY AccountId ORDER BY TranDate ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) as SlideAvg,
// MAGIC -- total # of the current and previous 2 transactions
// MAGIC COUNT(*) OVER (PARTITION BY AccountId ORDER BY TranDate ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) as SlideQty,
// MAGIC -- smallest of the current and previous 2 transactions
// MAGIC MIN(TranAmt) OVER (PARTITION BY AccountId ORDER BY TranDate ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) as SlideMin,
// MAGIC -- largest of the current and previous 2 transactions
// MAGIC MAX(TranAmt) OVER (PARTITION BY AccountId ORDER BY TranDate ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) as SlideMax,
// MAGIC -- total of the current and previous 2 transactions
// MAGIC SUM(TranAmt) OVER (PARTITION BY AccountId ORDER BY TranDate ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) as SlideTotal,
// MAGIC ROW_NUMBER() OVER (PARTITION BY AccountId ORDER BY TranDate) AS RN
// MAGIC FROM Sample.Transactions 
// MAGIC ORDER BY AccountId, TranDate, RN

// COMMAND ----------

val window = Window.partitionBy("AccountId").orderBy("TranDate").rowsBetween(-2,Window.currentRow)
val window2 = Window.partitionBy("AccountId").orderBy("TranDate")
sample_transactions
.withColumn("SlideAvg",avg("TranAmt").over(window))
.withColumn("SlideQty",count("*").over(window))
.withColumn("SlideMin",min("TranAmt").over(window))
.withColumn("SlideMax",max("TranAmt").over(window))
.withColumn("SlideTotal",sum("TranAmt").over(window))
.withColumn("RN", row_number().over(window2))
.orderBy("AccountId", "TranDate").show(true)


// COMMAND ----------

// MAGIC %md
// MAGIC * Logical Window

// COMMAND ----------

// MAGIC %sql
// MAGIC SELECT RowID,
// MAGIC FName,
// MAGIC Salary,
// MAGIC SUM(Salary) OVER (ORDER BY Salary ROWS UNBOUNDED PRECEDING) as SumByRows,
// MAGIC SUM(Salary) OVER (ORDER BY Salary RANGE UNBOUNDED PRECEDING) as SumByRange
// MAGIC 
// MAGIC FROM Sample.Logical
// MAGIC ORDER BY RowID

// COMMAND ----------

val window2 = Window.orderBy("Salary").rowsBetween(Window.unboundedPreceding, Window.currentRow)
val window = Window.orderBy("Salary").rangeBetween(Window.unboundedPreceding, Window.currentRow)

sample_logical.withColumn("SumByRows", sum("salary").over(window2))
.withColumn("SumByRange", sum("salary").over(window)).orderBy("RowID").show(true)

// COMMAND ----------

// MAGIC %sql
// MAGIC SELECT 
// MAGIC AccountId,
// MAGIC TranDate,
// MAGIC TranAmt,
// MAGIC ROW_NUMBER() OVER (PARTITION BY TranAmt ORDER BY TranDate) AS RN
// MAGIC FROM Sample.Transactions
// MAGIC ORDER BY TranAmt
// MAGIC LIMIT 10 

// COMMAND ----------

val window = Window.partitionBy("TranAmt").orderBy("TranDate")

sample_transactions.withColumn("RN", row_number().over(window)).orderBy("TranAmt").limit(10).show(true)

// COMMAND ----------

// MAGIC %md
// MAGIC Second exercise 

// COMMAND ----------

val window = Window.partitionBy("AccountId").orderBy("TranDate")
val windowRows=window.rowsBetween(Window.unboundedPreceding, -1) 


sample_transactions
.withColumn("RunLead",lead("TranAmt",2).over(window))
.withColumn("RunLag",lag("TranAmt",2).over(window))
.withColumn("RunFirstValue",first("TranAmt").over(windowRows))
.withColumn("RunLastValue",last("TranAmt").over(windowRows))
.withColumn("RunRowNumber",row_number().over(window))
.withColumn("RunDenseRank",dense_rank().over(window))
.orderBy("AccountId", "TranDate").show(true)

// COMMAND ----------

val window =Window.partitionBy(col("AccountId")).orderBy("TranAmt")
val windowRange=window.rangeBetween(-75 ,Window.currentRow)

sample_transactions
.withColumn("RunFirstValue",first("TranAmt").over(windowRange))
.withColumn("RunLastValue",last("TranAmt").over(windowRange))
.orderBy("AccountId", "TranAmt").show(true)

// COMMAND ----------

// MAGIC %md
// MAGIC Third exercise

// COMMAND ----------

val jdbcHostname = "bidevtestserver.database.windows.net"
val jdbcPort = 1433
val jdbcDatabase = "testdb"


val Customer = spark.read
  .format("jdbc")
  .option("url",s"jdbc:sqlserver://${jdbcHostname}:${jdbcPort};database=${jdbcDatabase}")
  .option("user","sqladmin")
  .option("password","$3bFHs56&o123$")
  .option("driver","com.microsoft.sqlserver.jdbc.SQLServerDriver")
  .option("query","SELECT * FROM SalesLT.Customer")
  .load()
val CustomerAddress = spark.read
  .format("jdbc")
  .option("url",s"jdbc:sqlserver://${jdbcHostname}:${jdbcPort};database=${jdbcDatabase}")
  .option("user","sqladmin")
  .option("password","$3bFHs56&o123$")
  .option("driver","com.microsoft.sqlserver.jdbc.SQLServerDriver")
  .option("query","SELECT * FROM SalesLT.CustomerAddress")
  .load()

display(Customer)


// COMMAND ----------

display(CustomerAddress)

// COMMAND ----------

// MAGIC %md
// MAGIC LEFT SEMI

// COMMAND ----------

val left_Semi = Customer.join(CustomerAddress, Customer("CustomerID") === CustomerAddress("CustomerID"), "leftsemi")
left_Semi.explain()

// COMMAND ----------

display(left_Semi)

// COMMAND ----------

val left_Anti = Customer.join(CustomerAddress, Customer("CustomerID") === CustomerAddress("CustomerID"), "leftanti")
left_Anti.explain()

// COMMAND ----------

display(left_Anti)

// COMMAND ----------

// MAGIC %md
// MAGIC Fourth exercise

// COMMAND ----------

val result_1 = Customer.join(CustomerAddress, Customer("CustomerID") === CustomerAddress("CustomerID")).drop(CustomerAddress("CustomerID"))
display(result_1)

// COMMAND ----------

val result_2 = Customer.join(CustomerAddress, Seq("CustomerID"))
display(result_2)

// COMMAND ----------

// MAGIC %md
// MAGIC Third exercise

// COMMAND ----------

import org.apache.spark.sql.functions.broadcast
val result_3=Customer.join(broadcast(CustomerAddress), Customer("CustomerID") === CustomerAddress("CustomerID"))
result_3.explain()

// COMMAND ----------

display(result_3)
