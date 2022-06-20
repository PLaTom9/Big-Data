package agh.wggios.analizadanych.datareader

import agh.wggios.analizadanych.sparksessionprovider.SparkSessionProvider
import org.apache.spark.sql.DataFrame

import java.nio.file.{Files, Paths}


class DataReader(path:String ) extends SparkSessionProvider{


  def read_csv(): DataFrame = if(Files.exists(Paths.get(this.path))) {
    logger.info("Reading csv")
    spark.read.format("csv").option("header", value = true).option("inferSchema",value = true).load(this.path)
  } else{
    logger.error("No such file")
    System.exit(0)
    spark.emptyDataFrame
  }

  def read_parquet(): DataFrame = {
    logger.info("Reading parquet")
    spark.read.format("parquet").load(this.path)
  }
}
