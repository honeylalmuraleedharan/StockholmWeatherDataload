/*
 * Copyright (c) 2019, TATA Consultancy Services Limited (TCSL) All rights reserved.
 *
 */
package org.tcs.spark.weather.dataload.loader

import java.util.Properties
import java.io.FileNotFoundException

import scala.util.Failure

import org.apache.log4j.Logger

import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}  
import org.apache.spark.sql.functions.lit

import org.tcs.spark.weather.dataload.utils.WeatherDataloadUtil
import org.tcs.spark.weather.dataload.schema._

/**
 * This class contain the methods which load Stockholm historical weather data
 * (air temperature and air pressure data) into respective Hive tables.
 * 
 * @author Honeylal Muraleedharan
 * @version 1.0
 */
object WeatherObservationLoader {
  /**
   *
   * This method load Air temperature observations data in Hive table
   * from Stockholm historical weather observation files, also responsible for 
   * cleansing the data.
   *
   * @param sparkSession
   * @param properties
   * @param log
   */
  def loadTemperatureObservations(spark: SparkSession, properties: Properties, log: Logger): Unit = {

    try {
      //-------------------------------------------------------------------
      // Reading air temperature observations data (year : 1756 to 1858 )
      //-------------------------------------------------------------------
      
      // enabled spark implicit conversions  
      import spark.implicits._
      
      val tempObs1756_1858DF = spark.read
        .textFile(properties.getProperty("air.temperature.observation.1756_1858.file.input.path"))
        // trim each line to resolve the extra space issue 
        .map(line => line.trim().split("\\s+")) 
        .map(col => TemperatureObservationSchema(
          col(0), col(1), col(2), col(3), col(4), col(5)))
        .withColumn("weather_station_type", lit("NaN"))

      //-------------------------------------------------------------------
      // Reading air temperature observations data (year : 1859 to 1960 )
      //-------------------------------------------------------------------

      val tempObs1859_1960DF = spark.read
        .textFile(properties.getProperty("air.temperature.observation.1859_1960.file.input.path"))
         // trim each line to resolve the extra space issue
        .map(line => line.trim().split("\\s+"))
        .map(col => TemperatureObservationSchema(
          col(0), col(1), col(2), col(3), col(4), col(5), col(6), col(7)))
        // Add weather_station_type
        .withColumn("weather_station_type", lit("NaN"))

      //-------------------------------------------------------------------
      // Reading air temperature observations data (year : 1961 to 2012 )
      //-------------------------------------------------------------------

      val tempObs1961_2012DF = spark.read
        .textFile(properties.getProperty("air.temperature.observation.1961_2012.file.input.path"))
         // trim each line to resolve the extra space issue
        .map(line => line.trim().split("\\s+"))
        .map(col => TemperatureObservationSchema(
          col(0), col(1), col(2), col(3), col(4), col(5), col(6),
          col(7), col(8)))
        // Add weather_station_type
        .withColumn("weather_station_type", lit("NaN"))

      //-----------------------------------------------------------------------------------------
      // Reading air temperature observations data (year : 2013 to 2017, station type: manual )
      //-----------------------------------------------------------------------------------------

      val tempObsManual2013_2017DF = spark.read
        .textFile(properties.getProperty("air.temperature.observation.manual.2013_2017.file.input.path"))
        // trim each line to resolve the extra space issue
        .map(line => line.trim().split("\\s+"))
        .map(col => TemperatureObservationSchema(
          col(0), col(1), col(2), col(3), col(4), col(5), col(6),
          col(7), col(8)))
        // Add weather_station_type
        .withColumn("weather_station_type", lit("manual"))

      //-----------------------------------------------------------------------------------------
      // Reading air temperature observations data (year : 2013 to 2017, station type: automatic )
      //-----------------------------------------------------------------------------------------

      val tempObsAutomatic2013_2017DF = spark.read
        .textFile(properties.getProperty("air.temperature.observation.automatic.2013_2017.file.input.path"))
        // trim each line to resolve the extra space issue
        .map(line => line.trim().split("\\s+"))
        .map(col => TemperatureObservationSchema(
          col(0), col(1), col(2), col(3), col(4), col(5), col(6),
          col(7), col(8)))
        // Add weather_station_type
        .withColumn("weather_station_type", lit("automatic"))

      // Joining all the input data to make as one data frame
      val joindAirTemperatureDF = tempObs1756_1858DF
        .union(tempObs1859_1960DF)
        .union(tempObs1961_2012DF)
        .union(tempObsManual2013_2017DF)
        .union(tempObsAutomatic2013_2017DF)

      val joindAirTemperatureDFCount = joindAirTemperatureDF.count()

      //  Removing rows if "year" or "month" or "day" columns not having any value.
      val airTemperatureObservationData = WeatherDataloadUtil.removeRowIfColumnsEmpty(
        joindAirTemperatureDF,
        Seq("year", "month", "day"))

      val airTempDFCountAfterRemovingInvalidRows = airTemperatureObservationData.count()

      /**
       * Save air temperature observation data to the External Hive table
       * configured in the property file.
       */
      val externalTableDir = properties.getProperty("air.temperature.observation.hive.location")

      // Create a Hive external Parquet table
      spark.sql("CREATE EXTERNAL TABLE IF NOT EXISTS weather_analysis_db.air_temperature_data(day String, "
        + "morning_temperature_observation_in_deg_C String, "
        + "noon_temperature_observation_in_deg_C String, "
        + "evening_temperature_observation_in_deg_C String, "
        + "day_max_temperature_in_deg_c String, "
        + "day_min_temperature_in_deg_c String, "
        + "day_estimated_temperature_mean_in_deg_c String, "
        + "weather_station_type String)"
        + "PARTITIONED BY (year String, month String)"
        + "STORED AS PARQUET "
        + s"LOCATION '$externalTableDir'")

      // Turn on flag for Hive Dynamic Partitioning
      spark.sqlContext.setConf("hive.exec.dynamic.partition", "true")
      spark.sqlContext.setConf("hive.exec.dynamic.partition.mode", "nonstrict")

      // Writing data into Hive partitioned table using DataFrame API
      airTemperatureObservationData.write
        .partitionBy("year", "month")
        .mode(SaveMode.Overwrite) // overwriting since its a historical full load
        .saveAsTable("weather_analysis_db.air_temperature_data")

      val hiveTableRowCount = spark
        .sql("SELECT count(*) FROM weather_analysis_db.air_temperature_data")
        .collect()(0)
        .getLong(0)


      //------------------------------------------------------------------------------
      // Data integrity check 
      //------------------------------------------------------------------------------
      log.info("Air Temperature Observation total row count  : " + joindAirTemperatureDFCount)
      log.info("Row count after data cleansing and transformation : " +
        airTempDFCountAfterRemovingInvalidRows)
      log.info("Hive data count : " + hiveTableRowCount)
      log.info(hiveTableRowCount + " out of "
        + airTempDFCountAfterRemovingInvalidRows + " rows are written into Hive table.")
      

    } catch {
      case fileNotFoundException: FileNotFoundException => {
        log.error("Input file not found while processing 'Air Temperature Observation' data")
        Failure(fileNotFoundException)
      }
      case exception: Exception => {
        log.error("Exception found while processing 'Air Temperature Observation' data. "
          + exception.getMessage())
        Failure(exception)
      }
    }
  }

  /**
   * This method load Air pressure observations(Barometer readings) data in Hive table
   * from the Stockholm historical weather observation files, also responsible for 
   * cleansing the data.
   *
   * @param sparkSession
   * @param properties
   * @param log
   */
  def loadPressureObservations(spark: SparkSession, properties: Properties, log: Logger): Unit = {

    try {
      //-------------------------------------------------------------------
      // Reading air pressure observations data (year : 1756 to 1858 )
      //-------------------------------------------------------------------
      import spark.implicits._

      val pressureObs1756_1858DF = spark.read
        .textFile(properties.getProperty("air.pressure.observation.1756_1858.file.input.path"))
        .map(line => line.trim().split("\\s+"))
        .map(col => AirPressureObservationSchema_1756_1858(
          col(0), col(1), col(2), col(3), col(4), col(5), col(6),
          col(7), col(8), "Swedish inches (29.69 mm)", "degC"))
        // Add remaining columns to match the Hive table columns
        .withColumn("air_pressure_reduced_to_0_degC_morning", lit("NaN"))
        .withColumn("air_pressure_reduced_to_0_degC_noon", lit("NaN"))
        .withColumn("air_pressure_reduced_to_0_degC_evening", lit("NaN"))
        .withColumn("air_pressure_morning", lit("NaN"))
        .withColumn("air_pressure_noon", lit("NaN"))
        .withColumn("air_pressure_evening", lit("NaN"))
        .withColumn("air_pressure_unit", lit("NaN"))
        .withColumn("weather_station_type", lit("NaN"))

      //-------------------------------------------------------------------
      // Reading air pressure observations data (year : 1859 to 1861 )
      //-------------------------------------------------------------------

      val pressureObs1859_1861DF = spark.read
        .textFile(properties.getProperty("air.pressure.observation.1859_1861.file.input.path"))
        .map(line => line.trim().split("\\s+"))
        .map(col => AirPressureObservationSchema_1859_1861(
          col(0), col(1), col(2), col(3), col(4), col(5), col(6),
          col(7), col(8), col(9), col(10), col(11),
          "Swedish inches (29.69 mm)", "degC"))
        // Add remaining columns to match the Hive table columns.
        .withColumn("air_pressure_morning", lit("NaN"))
        .withColumn("air_pressure_noon", lit("NaN"))
        .withColumn("air_pressure_evening", lit("NaN"))
        .withColumn("air_pressure_unit", lit("NaN"))
        .withColumn("weather_station_type", lit("NaN"))

      //------------------------------------------------------------------------------------
      // Reading air pressure observations data (year : 1862 to 1937 )
      //------------------------------------------------------------------------------------

      val pressureObs1862_1937DF = spark.read
        .textFile(properties.getProperty("air.pressure.observation.1862_1937.file.input.path"))
        .map(line => line.trim().split("\\s+"))
        .map(col => AirPressureObservationSchema_1859_1861(
          col(0), col(1), col(2), col(3), col(4), col(5), "mm Hg"))
        // Add remaining columns to match the Hive table columns.
        .withColumn("barometer_obs_morning", lit("NaN"))
        .withColumn("barometer_obs_noon", lit("NaN"))
        .withColumn("barometer_obs_evening", lit("NaN"))
        .withColumn("barometer_obs_unit", lit("NaN"))
        .withColumn("barometer_temperature_obs_morning", lit("NaN"))
        .withColumn("barometer_temperature_obs_noon", lit("NaN"))
        .withColumn("barometer_temperature_obs_evening", lit("NaN"))
        .withColumn("barometer_temperature_obs_unit", lit("NaN"))
        .withColumn("air_pressure_reduced_to_0_degC_morning", lit("NaN"))
        .withColumn("air_pressure_reduced_to_0_degC_noon", lit("NaN"))
        .withColumn("air_pressure_reduced_to_0_degC_evening", lit("NaN"))
        .withColumn("weather_station_type", lit("NaN"))

      //------------------------------------------------------------------------
      // Reading air pressure observations data (year : 1938 to 1960 )
      //------------------------------------------------------------------------

      val pressureObs1938_1960DF = spark.read
        .textFile(properties.getProperty("air.pressure.observation.1938_1960.file.input.path"))
        .map(line => line.trim().split("\\s+"))
        .map(col => AirPressureObservationSchema(
          col(0), col(1), col(2), col(3), col(4), col(5), "hPa"))
        // Add remaining columns to match the Hive table
        .withColumn("barometer_obs_morning", lit("NaN"))
        .withColumn("barometer_obs_noon", lit("NaN"))
        .withColumn("barometer_obs_evening", lit("NaN"))
        .withColumn("barometer_obs_unit", lit("NaN"))
        .withColumn("barometer_temperature_obs_morning", lit("NaN"))
        .withColumn("barometer_temperature_obs_noon", lit("NaN"))
        .withColumn("barometer_temperature_obs_evening", lit("NaN"))
        .withColumn("barometer_temperature_obs_unit", lit("NaN"))
        .withColumn("air_pressure_reduced_to_0_degC_morning", lit("NaN"))
        .withColumn("air_pressure_reduced_to_0_degC_noon", lit("NaN"))
        .withColumn("air_pressure_reduced_to_0_degC_evening", lit("NaN"))
        .withColumn("weather_station_type", lit("NaN"))

      //--------------------------------------------------------------------
      // Reading air pressure observations data (year : 1961 to 2012) 
      //--------------------------------------------------------------------

      val pressureObs1961_2012DF = spark.read
        .textFile(properties.getProperty("air.pressure.observation.1961_2012.file.input.path"))
        .map(line => line.trim().split("\\s+"))
        .map(col => AirPressureObservationSchema(
          col(0), col(1), col(2), col(3), col(4), col(5), "hPa"))
        // Add remaining columns to match the Hive table
        .withColumn("barometer_obs_morning", lit("NaN"))
        .withColumn("barometer_obs_noon", lit("NaN"))
        .withColumn("barometer_obs_evening", lit("NaN"))
        .withColumn("barometer_obs_unit", lit("NaN"))
        .withColumn("barometer_temperature_obs_morning", lit("NaN"))
        .withColumn("barometer_temperature_obs_noon", lit("NaN"))
        .withColumn("barometer_temperature_obs_evening", lit("NaN"))
        .withColumn("barometer_temperature_obs_unit", lit("NaN"))
        .withColumn("air_pressure_reduced_to_0_degC_morning", lit("NaN"))
        .withColumn("air_pressure_reduced_to_0_degC_noon", lit("NaN"))
        .withColumn("air_pressure_reduced_to_0_degC_evening", lit("NaN"))
        .withColumn("weather_station_type", lit("NaN"))

      //----------------------------------------------------------------------
      // Reading air pressure observations data (year : 2013 to 2017, manual)
      //----------------------------------------------------------------------

      val pressureObs2013_2017DF = spark.read
        .textFile(properties.getProperty("air.pressure.observation.manual.2013_2017.file.input.path"))
        .map(line => line.trim().split("\\s+"))
        .map(col => AirPressureObservationSchema(
          col(0), col(1), col(2), col(3), col(4), col(5), "hPa"))
        // Add remaining columns to match the Hive table
        .withColumn("barometer_obs_morning", lit("NaN"))
        .withColumn("barometer_obs_noon", lit("NaN"))
        .withColumn("barometer_obs_evening", lit("NaN"))
        .withColumn("barometer_obs_unit", lit("NaN"))
        .withColumn("barometer_temperature_obs_morning", lit("NaN"))
        .withColumn("barometer_temperature_obs_noon", lit("NaN"))
        .withColumn("barometer_temperature_obs_evening", lit("NaN"))
        .withColumn("barometer_temperature_obs_unit", lit("NaN"))
        .withColumn("air_pressure_reduced_to_0_degC_morning", lit("NaN"))
        .withColumn("air_pressure_reduced_to_0_degC_noon", lit("NaN"))
        .withColumn("air_pressure_reduced_to_0_degC_evening", lit("NaN"))
        .withColumn("weather_station_type", lit("manual"))

      //-------------------------------------------------------------------------
      // Reading air pressure observations data (year : 2013 to 2017, automatic)
      //-------------------------------------------------------------------------

      val pressureObs2013_2017AutomaticDF = spark.read
        .textFile(properties.getProperty("air.pressure.observation.automatic.1961_2012.file.input.path"))
        .map(line => line.trim().split("\\s+"))
        .map(col => AirPressureObservationSchema(
          col(0), col(1), col(2), col(3), col(4), col(5), "hPa"))
        // Add remaining columns to match the Hive table
        .withColumn("barometer_obs_morning", lit("NaN"))
        .withColumn("barometer_obs_noon", lit("NaN"))
        .withColumn("barometer_obs_evening", lit("NaN"))
        .withColumn("barometer_obs_unit", lit("NaN"))
        .withColumn("barometer_temperature_obs_morning", lit("NaN"))
        .withColumn("barometer_temperature_obs_noon", lit("NaN"))
        .withColumn("barometer_temperature_obs_evening", lit("NaN"))
        .withColumn("barometer_temperature_obs_unit", lit("NaN"))
        .withColumn("air_pressure_reduced_to_0_degC_morning", lit("NaN"))
        .withColumn("air_pressure_reduced_to_0_degC_noon", lit("NaN"))
        .withColumn("air_pressure_reduced_to_0_degC_evening", lit("NaN"))
        .withColumn("weather_station_type", lit("automatic"))

      // Joining all the input data to make as one data frame
      val joindAirPressureDF = WeatherDataloadUtil
        .createAirPressureDFWithUnifyColumnOrder(pressureObs1756_1858DF)
        .union(WeatherDataloadUtil.createAirPressureDFWithUnifyColumnOrder(pressureObs1859_1861DF))
        .union(WeatherDataloadUtil.createAirPressureDFWithUnifyColumnOrder(pressureObs1862_1937DF))
        .union(WeatherDataloadUtil.createAirPressureDFWithUnifyColumnOrder(pressureObs1938_1960DF))
        .union(WeatherDataloadUtil.createAirPressureDFWithUnifyColumnOrder(pressureObs1961_2012DF))
        .union(WeatherDataloadUtil.createAirPressureDFWithUnifyColumnOrder(pressureObs2013_2017DF))
        .union(WeatherDataloadUtil.createAirPressureDFWithUnifyColumnOrder(pressureObs2013_2017AutomaticDF))

      val joindAirPressureDFCount = joindAirPressureDF.count()

      //  Removing rows if "year" or "month" or "day" columns not having any value.
      val airPressureeObservationData = WeatherDataloadUtil.removeRowIfColumnsEmpty(
        joindAirPressureDF,
        Seq("year", "month", "day"))

      val airPressureDFCountAfterRemovingInvalidRows = airPressureeObservationData.count()

      //-------------------------------------------------------------------
      // Save pressure data to hive table
      //-------------------------------------------------------------------
      val externalTableDir = properties.getProperty("air.pressure.observation.hive.location")

      spark.sql("CREATE EXTERNAL TABLE IF NOT EXISTS weather_analysis_db.air_pressure_data (day String, "
        + "air_pressure_morning String, "
        + "air_pressure_noon String, "
        + "air_pressure_evening String, "
        + "air_pressure_unit String, "
        + "barometer_obs_morning String, "
        + "barometer_obs_noon String, "
        + "barometer_obs_evening String, "
        + "barometer_obs_unit String, "
        + "barometer_temperature_obs_morning String, "
        + "barometer_temperature_obs_noon String, "
        + "barometer_temperature_obs_evening String, "
        + "barometer_temperature_obs_unit String, "
        + "air_pressure_reduced_to_0_degC_morning String, "
        + "air_pressure_reduced_to_0_degC_noon String, "
        + "air_pressure_reduced_to_0_degC_evening String, "
        + "weather_station_type  String)"
        + "PARTITIONED BY (year String, month String)"
        + "STORED AS PARQUET "
        + s"LOCATION '$externalTableDir'")

      // Turn on flag for Hive Dynamic Partitioning
      spark.sqlContext.setConf("hive.exec.dynamic.partition", "true")
      spark.sqlContext.setConf("hive.exec.dynamic.partition.mode", "nonstrict")

      // Writing data into Hive partitioned table using DataFrame API
      airPressureeObservationData.write.partitionBy("year", "month")
        .mode(SaveMode.Overwrite)
        .saveAsTable("weather_analysis_db.air_pressure_data")

      val hiveTableRowCount = spark
        .sql("SELECT count(*) FROM weather_analysis_db.air_pressure_data")
        .collect()(0)
        .getLong(0)

      //------------------------------------------------------------------------------
      // Data integrity check 
      //------------------------------------------------------------------------------
      log.info("Air Pressure Observation total row count  : " + joindAirPressureDFCount)
      log.info("Row count after data cleansing and transformation : " +
        airPressureDFCountAfterRemovingInvalidRows)
      log.info("Hive data count : " + hiveTableRowCount)
      log.info(hiveTableRowCount + " out of "
        + airPressureDFCountAfterRemovingInvalidRows + " rows are written into Hive table.")
  
    } catch {
      case fileNotFoundException: FileNotFoundException => {
        log.error("Input file not found while processing 'Air Pressure Observation' data")
        Failure(fileNotFoundException)
      }
      case exception: Exception => {
        log.error("Exception found while processing 'Air Pressure Observation' data. "
          + exception.getMessage())
        Failure(exception)
      }
    }
  }
}