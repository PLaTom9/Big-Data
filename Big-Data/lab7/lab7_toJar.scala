package org.example
import org.apache.spark.sql.SparkSession
import org.apache.commons.io.IOUtils
import org.apache.spark.sql.functions.col

import java.net.URL


object lab7 {
  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession.builder().config("spark.master", "local").getOrCreate()
    val urlfile=new URL("https://raw.githubusercontent.com/lrjoshi/webpage/master/public/post/c159s.csv")
    import spark.implicits._
    val testcsvgit = IOUtils.toString(urlfile,"UTF-8").lines.toList.toDS()
    val df = spark.read.option("header", value = true).option("inferSchema", value = true).csv(testcsvgit)
    val df2=df.withColumn("NewColumn",col("Titer")+3).withColumn("Titer",col("Titer").cast("Integer"))
    df2.show
  }
}
