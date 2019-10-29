/*
 * Copyright (c) 2019, TATA Consultancy Services Limited (TCSL) All rights reserved.
 *
 */
package org.tcs.spark.weather.dataload.utils

import java.util.Properties

import scala.io.Source

import org.apache.spark.sql.DataFrame
import java.io.IOException
import java.io.FileNotFoundException

/**
 * This class contains the utility methods.
 *
 * @author Honeylal Muraleedharan
 * @version 1.0
 */
object WeatherDataloadUtil {

  /**
   * Reads property file from application resource folder and return Properties 
   *
   * @args fileName property file name
   * @return Properties
   * @throws Exception
   */
  @throws(classOf[Exception])
  def getProperties (fileName: String): Properties = {
    var properties: Properties = null
    val propertyFileUrl = getClass.getResource("/" + fileName)
    if (propertyFileUrl != null) { 
        val propSource = Source.fromURL(propertyFileUrl)
        properties = new Properties()
        properties.load(propSource.bufferedReader()) 
    } 
    if(properties == null) {
      throw new Exception("Properties file cannot be loaded");
    }
    properties
  }
  
   /**
   *
   * This function will return a new DataFrame that drops rows containing
   * empty string or null or NaN values in the specified columns
   *
   * @param inputDF : input DataFrame
   * @param colums : list of column need to check for null or NaN
   */
  def removeRowIfColumnsEmpty(inputDF: DataFrame, columns: Seq[String]): DataFrame = {
    // replace empty string to null in the specified columns
    inputDF.na.replace(columns, Map("" -> null))
      .na.drop() // return a new dataframe that drops rows containing any null or NaN value
  }
  
  /**
   * This function will return a new Air Pressure DataFrame with unify column order. 
   * 
   * @param sourceDF (Air Pressure DataFrame)
   * @return dataFame with ordered columns
   */
  def createAirPressureDFWithUnifyColumnOrder (sourceDF: DataFrame): DataFrame = {
    sourceDF.select(
      "day",
      "air_pressure_morning",
      "air_pressure_noon",
      "air_pressure_evening",
      "air_pressure_unit",
      "barometer_obs_morning",
      "barometer_obs_noon",
      "barometer_obs_evening",
      "barometer_obs_unit",
      "barometer_temperature_obs_morning",
      "barometer_temperature_obs_noon",
      "barometer_temperature_obs_evening",
      "barometer_temperature_obs_unit",
      "air_pressure_reduced_to_0_degC_morning",
      "air_pressure_reduced_to_0_degC_noon",
      "air_pressure_reduced_to_0_degC_evening",
      "weather_station_type",
      "year",
      "month")
  }
}