package agh.wggios.analizadanych

import agh.wggios.analizadanych.caseclass.FlightCaseClass
import agh.wggios.analizadanych.datareader.DataReader
import agh.wggios.analizadanych.sparksessionprovider.SparkSessionProvider
import agh.wggios.analizadanych.transformations.Transformations

object Main extends SparkSessionProvider{

  def main(args: Array[String]): Unit = {
    logger.info("Starting application")
    import spark.implicits._
    val df =new DataReader(args(0)).read_csv().as[FlightCaseClass]
    df.filter(row => new Transformations().OnlyBigAirports(row)).show()

    df.groupByKey(x => x.DEST_COUNTRY_NAME).reduceGroups((l, r) => new Transformations().sum2(l, r))
      .show()

  }
}
