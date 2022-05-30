package agh.wggios.analizadanych.sparksessionprovider

import org.apache.spark.sql.SparkSession

class SparkSessionProvider {
  val spark: SparkSession = SparkSession.builder().config("spark.master", "local").getOrCreate()
}
