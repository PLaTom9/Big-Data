package agh.wggios.analizadanych.transformations

import agh.wggios.analizadanych.caseclass.FlightCaseClass

class Transformations {
  def OnlyBigAirports(flight_row: FlightCaseClass): Boolean ={
    flight_row.count > 100
  }
  def sum2(left:FlightCaseClass, right:FlightCaseClass) = {
    FlightCaseClass(left.DEST_COUNTRY_NAME, null, left.count + right.count)
  }
}
