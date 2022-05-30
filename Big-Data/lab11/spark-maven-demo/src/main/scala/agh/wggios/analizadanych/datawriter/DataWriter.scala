package agh.wggios.analizadanych.datawriter
import agh.wggios.analizadanych.sparksessionprovider.SparkSessionProvider
import org.apache.spark.sql.DataFrame

import java.nio.file.{Files, Paths}

class DataWriter(path:String,df:DataFrame) extends SparkSessionProvider {
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
