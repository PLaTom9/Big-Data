package agh.wggios.analizadanych.datareader
import agh.wggios.analizadanych.caseclass.FlightCaseClass
import agh.wggios.analizadanych.sparksessionprovider.SparkSessionProvider
import org.apache.spark.sql.{DataFrame, Dataset, Encoder}

import java.nio.file.{Files, Paths}

class DataReader(path:String ) extends SparkSessionProvider{

   /* def read_csv_case[T: Encoder[T]]()( implicit encoder: Encoder[T]): Dataset[T]  = if(Files.exists(Paths.get(this.path))){
      import spark.implicits._
      spark.read.format("csv").option("header", value = true).option("inferSchema",value = true).load(this.path).as[T]
    } else{
    println("No such file")
    System.exit(0)
    spark.emptyDataset[T]
  }*/
  def read_csv(): DataFrame = if(Files.exists(Paths.get(this.path))) {
    spark.read.format("csv").option("header", value = true).option("inferSchema",value = true).load(this.path)
  } else{
    println("No such file")
    System.exit(0)
    spark.emptyDataFrame
  }

  def read_parquet(): DataFrame = {
    spark.read.format("parquet").load(this.path)
  }
}
