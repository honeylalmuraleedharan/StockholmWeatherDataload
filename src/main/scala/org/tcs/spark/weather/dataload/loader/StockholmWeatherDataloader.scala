/*
 * Copyright (c) 2019, TATA Consultancy Services Limited (TCSL) All rights reserved.
 *
 */
package org.tcs.spark.weather.dataload.loader

import java.util.Properties

import scala.util.Failure

import org.apache.log4j.Logger

import org.tcs.spark.weather.dataload.common.SparkConfiguration
import org.tcs.spark.weather.dataload.utils.WeatherDataloadUtil
import java.io.FileNotFoundException

/**
  * This is the main class contain the Entry point of the application
  *
  * @author Honeylal Muraleedharan
  * @version 1.0
  */
object StockholmWeatherDataloader extends SparkConfiguration {
  // logger
  val log = Logger.getLogger(getClass.getName)

  /**
    * Entry point to the application
    *
    * @param args
    */
  def main(args: Array[String]) {

    // Naming spark program
    spark.conf.set("spark.app.name", "Stockholm Weather Dataloader")

    try {
      // Reading property file
      val properties = WeatherDataloadUtil.getProperties("WeatherDataload.properties")

      log.info("Air temperature observations dataload started...")
      WeatherObservationLoader.loadTemperatureObservations(
        spark,
        properties,
        log)

      log.info("Air pressure observations dataload started...")
      WeatherObservationLoader.loadPressureObservations(
        spark,
        properties,
        log)

    } catch {
      case exception: Exception => {
        log.error("Exception found :  " + exception.getMessage())
        Failure(exception)
      }
    } finally {
      //Stop the Spark context
      stopSpark()
    }
  }
}