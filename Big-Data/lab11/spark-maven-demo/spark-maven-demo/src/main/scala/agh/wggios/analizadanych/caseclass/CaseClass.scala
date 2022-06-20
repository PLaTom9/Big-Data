package agh.wggios.analizadanych.caseclass

case class FlightCaseClass(DEST_COUNTRY_NAME: String, ORIGIN_COUNTRY_NAME: String, count: BigInt)

case class RetailCaseClass(InvoiceNo: BigInt, StockCode: BigInt, Description: String, Quantity: Int,InvoiceDate: String, UnitPrice: BigDecimal, CustomerId: BigDecimal, Country: String )