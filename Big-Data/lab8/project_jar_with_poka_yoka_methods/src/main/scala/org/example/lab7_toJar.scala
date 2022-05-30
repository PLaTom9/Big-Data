package org.example
import org.apache.spark.sql.expressions.{UserDefinedFunction, Window}
import org.apache.spark.sql.{DataFrame, SparkSession, functions}
import java.nio.file.Files
import org.apache.spark.sql.functions.{col, udf}
import org.apache.spark.sql.types.StructType

import java.nio.file.Paths


class SparkSessionProvider{
  val spark: SparkSession = SparkSession.builder().config("spark.master", "local").getOrCreate()
}

class DataReader(file:String) extends SparkSessionProvider {

  private val fileName:String = file
  private var schema:StructType = StructType(List())
  private var isSchema : Boolean = false

  def this(file:String, schema:StructType){
    this(file)
    this.schema = schema
    this.isSchema = true
  }

  def ReadData(): DataFrame = if(Files.exists(Paths.get(this.fileName))) {
    spark.read.option("header", value = true).option("inferSchema",this.isSchema).csv(this.fileName)
  } else{
    println("No such file")
    System.exit(0)
    spark.emptyDataFrame
  }

}

class Pipeline(dataframe:DataFrame) extends SparkSessionProvider {
  private val df: DataFrame = dataframe

  def CastToInt(ColumnName: String): DataFrame ={
    if(df.columns.contains(ColumnName))
      this.df.withColumn(ColumnName, col(ColumnName).cast("Integer"))
    else{
      println("No such Column")
      System.exit(0)
      spark.emptyDataFrame
    }
  }

  def OccurrencesOfWordInRow(ColumnName:String, Word:String): DataFrame ={
    if(df.columns.contains(ColumnName))
      this.df.withColumn("WordCount", countAll(Word)(col(ColumnName)))
    else{
      println("No such Column")
      System.exit(0)
      spark.emptyDataFrame
    }
  }
  def countAll(pattern: String): UserDefinedFunction = udf((s: String) => pattern.r.findAllIn(s).size)

  def DropNullsInColumns(ColumnNames:Array[String]): DataFrame ={
    var temp =  this.df
    for( name <- ColumnNames){
      if(df.columns.contains(name))
        temp=temp.drop(name)
      else
        println("No such Column:" + name)
    }
    temp
  }
  def CalculateRatio(column1:String, column2:String) : DataFrame = {
    if(df.columns.contains(column1) && df.columns.contains(column2)  ){
      this.df.withColumn("Ratio",col(column1)/col(column2))
    }
    else{
      println("No such Column")
      System.exit(0)
      spark.emptyDataFrame
    }
  }
  def WindowedFunction(partitionByColumn:String,orderByColumn: String, sumColumn: String) : DataFrame = {
    if(df.columns.contains(orderByColumn) && df.columns.contains(sumColumn) && df.columns.contains(partitionByColumn) ) {
      val window = Window.partitionBy(partitionByColumn).orderBy(orderByColumn).rangeBetween(Window.unboundedPreceding, Window.currentRow)
      this.df.withColumn("SumByRows", functions.sum(sumColumn).over(window))
    }else{
      println("No such Column")
      System.exit(0)
      spark.emptyDataFrame
    }

  }

  def WriteResult(path:String):Unit={
    if(!this.df.isEmpty) {
      if(Files.exists(Paths.get(path))){
        println("path file: " +path + " already exists.")
        System.exit(0)
      }
      this.df.write.parquet(path)
    } else{
      println("There was a problem and the dataframe is empty")
      System.exit(0)
    }

  }

}

object lab8 {
  def main(args: Array[String]): Unit = {
    val fileName = args(0)
    val dataReader = new DataReader(fileName)
    val df = dataReader.ReadData()
    val pipeline = new Pipeline(df)
    pipeline.CalculateRatio("balance","duration").show
    pipeline.CastToInt("age").show
    val colToDrop  = Array("age","day")
    pipeline.DropNullsInColumns(colToDrop).show
    pipeline.WindowedFunction("month","day","balance").show
    pipeline.OccurrencesOfWordInRow("marital","married").show
    pipeline.WriteResult("out.parquet")

  }
}