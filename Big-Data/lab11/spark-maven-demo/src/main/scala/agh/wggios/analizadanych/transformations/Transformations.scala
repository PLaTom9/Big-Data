package agh.wggios.analizadanych.transformations

import agh.wggios.analizadanych.caseclass.FlightCaseClass
import org.apache.log4j.Logger


class Transformations {
  @transient lazy val logger: Logger = Logger.getLogger(getClass.getName)
  def OnlyBigAirports(flight_row: FlightCaseClass): Boolean ={
    logger.info("Making transformations")
    flight_row.count > 100
  }
  def sum2(left:FlightCaseClass, right:FlightCaseClass): FlightCaseClass = {
    FlightCaseClass(left.DEST_COUNTRY_NAME, null, left.count + right.count)
  }
}
