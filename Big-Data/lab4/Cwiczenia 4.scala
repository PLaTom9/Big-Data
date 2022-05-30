// Databricks notebook source
// MAGIC %md 
// MAGIC Wykorzystaj dane z bazy 'bidevtestserver.database.windows.net'
// MAGIC ||
// MAGIC |--|
// MAGIC |SalesLT.Customer|
// MAGIC |SalesLT.ProductModel|
// MAGIC |SalesLT.vProductModelCatalogDescription|
// MAGIC |SalesLT.ProductDescription|
// MAGIC |SalesLT.Product|
// MAGIC |SalesLT.ProductModelProductDescription|
// MAGIC |SalesLT.vProductAndDescription|
// MAGIC |SalesLT.ProductCategory|
// MAGIC |SalesLT.vGetAllCategories|
// MAGIC |SalesLT.Address|
// MAGIC |SalesLT.CustomerAddress|
// MAGIC |SalesLT.SalesOrderDetail|
// MAGIC |SalesLT.SalesOrderHeader|

// COMMAND ----------

//INFORMATION_SCHEMA.TABLES

val jdbcHostname = "bidevtestserver.database.windows.net"
val jdbcPort = 1433
val jdbcDatabase = "testdb"

val tabela = spark.read
  .format("jdbc")
  .option("url",s"jdbc:sqlserver://${jdbcHostname}:${jdbcPort};database=${jdbcDatabase}")
  .option("user","sqladmin")
  .option("password","$3bFHs56&o123$")
  .option("driver","com.microsoft.sqlserver.jdbc.SQLServerDriver")
  .option("query","SELECT * FROM INFORMATION_SCHEMA.TABLES")
  .load()
display(tabela)

// COMMAND ----------

// MAGIC %md
// MAGIC 1. Pobierz wszystkie tabele z schematu SalesLt i zapisz lokalnie bez modyfikacji w formacie delta

// COMMAND ----------

import org.apache.spark.sql.functions.col
val table_names= tabela.select(col("TABLE_NAME")).filter('TABLE_SCHEMA === "SalesLT").as[String].collect.toList
val i = List()
for ( i <- table_names){
  val tabela = spark.read
  .format("jdbc")
  .option("url",s"jdbc:sqlserver://${jdbcHostname}:${jdbcPort};database=${jdbcDatabase}")
  .option("user","sqladmin")
  .option("password","$3bFHs56&o123$")
  .option("driver","com.microsoft.sqlserver.jdbc.SQLServerDriver")
  .option("query",s"SELECT * FROM SalesLT.$i")
  .load()
  tabela.write.format("delta").mode("overwrite").saveAsTable(i)
}


// COMMAND ----------

// MAGIC %md
// MAGIC  Uzycie Nulls, fill, drop, replace, i agg
// MAGIC  * W każdej z tabel sprawdź ile jest nulls w rzędach i kolumnach
// MAGIC  * Użyj funkcji fill żeby dodać wartości nie występujące w kolumnach dla wszystkich tabel z null
// MAGIC  * Użyj funkcji drop żeby usunąć nulle, 
// MAGIC  * wybierz 3 dowolne funkcje agregujące i policz dla TaxAmt, Freight, [SalesLT].[SalesOrderHeader]
// MAGIC  * Użyj tabeli [SalesLT].[Product] i pogrupuj według ProductModelId, Color i ProductCategoryID i wylicz 3 wybrane funkcje agg() 
// MAGIC    - Użyj conajmniej dwóch overloded funkcji agregujących np z (Map)

// COMMAND ----------

// MAGIC %md
// MAGIC W każdej z tabel sprawdź ile jest nulls w rzędach i kolumnach

// COMMAND ----------

// W każdej z tabel sprawdź ile jest nulls w rzędach i kolumnach
import org.apache.spark.sql.Column
import org.apache.spark.sql.functions.{col,when, count}
def NullColCounter(columns:Array[String]):Array[Column]={
    columns.map(c=>{
      count(when(col(c).isNull,c)).alias(c)
    })
}
val i = List()
var new_tables = table_names.map(x=>x.toLowerCase())
for ( i <- new_tables){
  val df =spark.read.format("delta").option("header","true").option("inferSchema","true").load(s"dbfs:/user/hive/warehouse/$i")
  df.select(NullColCounter(df.columns):_*).show()
}

// COMMAND ----------

import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

val i = List()
var new_tables = table_names.map(x=>x.toLowerCase())
for ( i <- new_tables){
  val df =spark.read.format("delta").option("header","true").option("inferSchema","true").load(s"dbfs:/user/hive/warehouse/$i")
  println(i ,df.agg(count("*")).head().get(0))
}



// COMMAND ----------

// MAGIC %md
// MAGIC Użyj funkcji fill żeby dodać wartości nie występujące w kolumnach dla wszystkich tabel z null

// COMMAND ----------

import org.apache.spark.sql.DataFrame
val i = List()
var new_tables = table_names.map(x=>x.toLowerCase())

def null_replace(df_toReplc:DataFrame):DataFrame = {
  println("Inside the null_replace")
  var df:DataFrame = df_toReplc
  for (i <- df.columns){
    df = df.schema(i).dataType match {
      case DoubleType => (df.na.fill(-0.0,Seq(i)))
      case StringType => (df.na.fill("NS",Seq(i)))
      case IntegerType => (df.na.fill(-999,Seq(i)))
      case LongType => (df.na.fill(-99999,Seq(i)))
      case BooleanType => (df.na.fill(0,Seq(i)))
      case TimestampType => (df.withColumn(i,when(df.col(i).isNull,to_date(lit("9999-01-01"),"yyyy-MM-dd")).otherwise(col(i))))
      case DateType => (df.withColumn(i,when(df.col(i).isNull,to_date(lit("9999-01-01"),"yyyy-MM-dd")).otherwise(col(i))))
      case _ => (df.na.fill(0,Seq(i)))
    }    
  }
  df
}

for ( i <- new_tables){
  val df =spark.read.format("delta").option("header","true").option("inferSchema","true").load(s"dbfs:/user/hive/warehouse/$i")
  val df_new = null_replace(df)
  println(df_new.show())
}


// COMMAND ----------

// MAGIC %md
// MAGIC Użyj funkcji drop żeby usunąć nulle,

// COMMAND ----------

val i = List()
var new_tables = table_names.map(x=>x.toLowerCase())

for ( i <- new_tables){
  val df =spark.read.format("delta").option("header","true").option("inferSchema","true").load(s"dbfs:/user/hive/warehouse/$i")
  val df_new = df.na.drop()
  println(df_new.show())
}


// COMMAND ----------

// MAGIC %md
// MAGIC wybierz 3 dowolne funkcje agregujące i policz dla TaxAmt, Freight, [SalesLT].[SalesOrderHeader]

// COMMAND ----------

val df =spark.read.format("delta").option("header","true").option("inferSchema","true").load(s"dbfs:/user/hive/warehouse/salesorderheader")
df.agg(avg("TaxAmt").as("TaxAmt:Avg"),skewness ("TaxAmt").as("TaxAmt:Skewness "),stddev_samp("TaxAmt").as("TaxAmt:Stddev_samp"),avg("Freight").as("Freight:Avg"),skewness ("Freight").as("Freight:Skewness "),stddev_samp("Freight").as("Freight:Stddev_samp") ).show()


// COMMAND ----------

// MAGIC %md
// MAGIC Użyj tabeli [SalesLT].[Product] i pogrupuj według ProductModelId, Color i ProductCategoryID i wylicz 3 wybrane funkcje agg()
// MAGIC <br>Użyj conajmniej dwóch overloded funkcji agregujących np z (Map)

// COMMAND ----------

val df =spark.read.format("delta").option("header","true").option("inferSchema","true").load(s"dbfs:/user/hive/warehouse/product")

display(df.groupBy("ProductModelId", "Color", "ProductCategoryID")
                         .agg("Color" -> "count", "StandardCost"->"sum", "weight"->"max"))


// COMMAND ----------

val df =spark.read.format("delta").option("header","true").option("inferSchema","true").load(s"dbfs:/user/hive/warehouse/salesorderdetail")
display(df)


// COMMAND ----------

// MAGIC %md
// MAGIC Stwórz 3 funkcje UDF do wybranego zestawu danych, 
// MAGIC 
// MAGIC Dwie funkcje działające na liczbach, int, double 
// MAGIC 
// MAGIC Jedna funkcja na string 

// COMMAND ----------

val LineTotalMy = udf((UnitPrice: Double, OrderQty: Integer,UnitPriceDiscount: Double) => (OrderQty * (1-UnitPriceDiscount)* UnitPrice)*100.round/ 100.toDouble)

val ConvertToPln = udf((UnitPrice: Double) => (UnitPrice*4.30)*10000.round/ 10000.toDouble)

def DeletePause = udf((string: String) => (string.replaceAll("-", "")))

val df_new = df.select("*").withColumn("NewTotalLine",LineTotalMy(col("UnitPrice"),col("OrderQty"),col("UnitPriceDiscount"))).withColumn("UnitPricePln",ConvertToPln(col("UnitPrice"))).withColumn("RowguidTransform",DeletePause(col("rowguid")))
display(df_new)

// COMMAND ----------

// MAGIC %md
// MAGIC Flatten json, wybieranie atrybutów z pliku json. 

// COMMAND ----------

import org.apache.spark.sql.types.{StructType,ArrayType}
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{col,explode_outer}



  def renameDataFrameCols(df: DataFrame, colNames: Map[String,String]) : DataFrame ={
    val newDf = df.select(df.columns.map(x => col(x).alias(colNames.getOrElse(x,x))): _*)
   newDf
  }


  def updateColumnNames(df: DataFrame, index: Int): DataFrame = {

    val allCols = df.columns
    val newCols = allCols.map(column => (column, s"${column}*${index}")).toMap
    val df_temp = df.transform(renameDataFrameCols(_,newCols))
    df_temp
  }


  def flattenJson(df_arg: DataFrame, index: Int = 1) : DataFrame = {
    
    val df = if(index == 1) updateColumnNames(df_arg, index) else df_arg
    
    val fields = df.schema.fields
    
    for ( field <- fields){
      val dataType = field.dataType
      val columnName = field.name
      
      dataType match{
        case structType: StructType => {
          var currentCol = columnName
          val appendStr = currentCol
          
          val dataTypeStr = df.schema(currentCol).dataType.toString
          
          val df_temp = if(dataTypeStr.contains(columnName)){
            currentCol = currentCol +"temp"
            df.withColumnRenamed(columnName, columnName + "temp")
          } else df
          
          val dfBefore = df_temp.select(col = s"${currentCol}.*")
          
          val newCols = dfBefore.columns
          
          val begin_index = appendStr.lastIndexOf("*")
          val end_index = appendStr.length
          val level = appendStr.substring(begin_index+1, end_index)
          val nextLevel = level.toInt + 1
          
          val customCols = newCols.map(field => (field, s"${appendStr}->${field}*${nextLevel}")).toMap
          val df_temp2 = df_temp.select("*", cols=s"${currentCol}.*").drop(currentCol)
          
          val df_temp3 = df_temp2.transform(renameDataFrameCols(_,customCols))
          
          return flattenJson(df_temp3, index +1)
        }
        
         case arrayType: ArrayType =>{
          val df_temp = df.withColumn(columnName, explode_outer(col(columnName)))
          
          return flattenJson(df_temp, index+1)
        }
        case _ =>
      }
    }
    df
  }


val path = "dbfs:/FileStore/tables/brzydki.json"
val jsonDF = spark.read.option("multiline","true").json(path)
val df = flattenJson(jsonDF)
display(df)


// COMMAND ----------

// MAGIC %md
// MAGIC https://medium.com/@thomaspt748/how-to-flatten-json-files-dynamically-using-apache-spark-scala-version-9cc9560631dd
