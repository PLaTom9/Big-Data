package agh.wggios.analizadanych.sparksessionprovider

import org.apache.log4j.Logger
import org.apache.spark.sql.SparkSession

class SparkSessionProvider {
  val spark: SparkSession = SparkSession.builder().config("spark.master", "local").getOrCreate()

  @transient lazy val logger: Logger = Logger.getLogger(getClass.getName)
}
