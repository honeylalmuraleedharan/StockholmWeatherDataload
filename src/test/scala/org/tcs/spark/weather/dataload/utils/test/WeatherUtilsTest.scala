/*
 * Copyright (c) 2019, TATA Consultancy Services Limited (TCSL) All rights reserved.
 *
 */
package org.tcs.spark.weather.dataload.utils.test

import java.io.InputStream

import org.apache.spark.sql.SparkSession

import org.scalatest.{ Outcome, fixture }

import org.tcs.spark.weather.dataload.schema.TemperatureObservationSchema
import org.tcs.spark.weather.dataload.utils.WeatherDataloadUtil

/**
 * For unit testing Weather Dataload Util functions.
 *
 * @author Honeylal Muraleedharan
 */
class WeatherUtilsTest extends fixture.FunSuite {
  type FixtureParam = SparkSession

  def withFixture(test: OneArgTest): Outcome = {
    val sparkSession = SparkSession.builder
      .appName("TestRun")
      .master("local")
      .getOrCreate()
    try {
      withFixture(test.toNoArgTest(sparkSession))
    } finally sparkSession.stop
  }

  /**
   * Testing removeRowIfColumnsEmpty function.
   * The sample file having 20 rows, out of 3 row are invalid.
   *
   */
  test("Testing removeRowIfColumnsEmpty.") { spark =>
    val TempObsRDD = spark.sparkContext
      .parallelize(getInputData("/data/temp_obs_2013_2017_manual_sample.txt"), 2)
      .map(item => item.split("\\s+"))
      .map(col => TemperatureObservationSchema(
        col(0), col(1), col(2), col(3), col(4), col(5), col(6),
        col(7), col(8)))
    // the sample file having 20 row
    assert(TempObsRDD.count === 20)

    val TempObsDF = spark.createDataFrame(TempObsRDD)
    val TempObsFinalDF = WeatherDataloadUtil.removeRowIfColumnsEmpty(
      TempObsDF,
      Seq("year", "month", "day"))
    // checking if the 3 invalid record got removed or not
    assert(TempObsFinalDF.count === 17)

  }

  /**
   * Loading the sample using InputStream and return as Seq[String]
   */
  private def getInputData(name: String): Seq[String] = {
    val is: InputStream = getClass.getResourceAsStream(name)
    scala.io.Source.fromInputStream(is).getLines.toSeq
  }
}