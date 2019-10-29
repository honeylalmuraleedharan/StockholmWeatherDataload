/*
 * Copyright (c) 2019, TATA Consultancy Services Limited (TCSL) All rights reserved.
 *
 */

package org.tcs.spark.weather.dataload.common

import org.apache.spark.sql
import org.apache.spark.sql.SparkSession

/**
  * Trait that handle spark session. This is used across the project
  *
  * @author Honeylal Muraleedharan
  * @version 1.0
  *
  */
trait SparkConfiguration {

  // here the spark session lazily initialized
  lazy val spark =
    SparkSession
      .builder()
      .enableHiveSupport()
      .getOrCreate()

  /**
    *  Method to stop spark session
    */
  def stopSpark(): Unit = spark.stop()
}