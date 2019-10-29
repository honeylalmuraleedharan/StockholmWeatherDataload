/*
 * Copyright (c) 2019, TATA Consultancy Services Limited (TCSL) All rights reserved.
 *
 */
package org.tcs.spark.weather.dataload.schema

/**
  * Case class for reading daily air pressure observation data
  * Missing observations are flagged with 'NaN'.
  *
  * @author Honeylal Muraleedharan
  * @version 1.0
  */
case class TemperatureObservationSchema(
  year:                                     String,
  month:                                    String,
  day:                                      String,
  morning_temperature_observation_in_deg_C: String = "NaN",
  noon_temperature_observation_in_deg_C:    String = "NaN",
  evening_temperature_observation_in_deg_C: String = "NaN",
  day_max_temperature_in_deg_c:             String = "NaN",
  day_min_temperature_in_deg_c:             String = "NaN",
  day_estimated_temperature_mean_in_deg_c:  String = "NaN")

/**
  * Case class for reading daily air pressure observation data in the year 1756 to 1858.
  * Missing observations are flagged with 'NaN'.
  * 3 daily obs taken in this order morning, noon, evening
  *
  * @author Honeylal Muraleedharan
  * @version 1.0
  */
case class AirPressureObservationSchema_1756_1858(
  year:                                   String,
  month:                                  String,
  day:                                    String,
  barometer_obs_morning:                  String = "NaN",
  barometer_temperature_obs_morning:      String = "NaN",
  barometer_obs_noon:                     String = "NaN",
  barometer_temperature_obs_noon:         String = "NaN",
  barometer_obs_evening:                  String = "NaN",
  barometer_temperature_obs_evening:      String = "NaN",
  barometer_obs_unit:                     String = "NaN",
  barometer_temperature_obs_unit:         String = "NaN")

/**
  * Case class for reading daily air pressure observation data in the year 1759 to 1861
  * Missing observations are flagged with 'NaN'.
  * 3 daily obs taken in this order morning, noon, evening
  *
  * @author Honeylal Muraleedharan
  * @version 1.0
  */
case class AirPressureObservationSchema_1859_1861(
  year:                                   String,
  month:                                  String,
  day:                                    String,
  barometer_obs_morning:                  String = "NaN",
  barometer_temperature_obs_morning:      String = "NaN",
  air_pressure_reduced_to_0_degC_morning: String = "NaN",
  barometer_obs_noon:                     String = "NaN",
  barometer_temperature_obs_noon:         String = "NaN",
  air_pressure_reduced_to_0_degC_noon:    String = "NaN",
  barometer_temperature_obs_evening:      String = "NaN",
  barometer_obs_evening:                  String = "NaN",
  air_pressure_reduced_to_0_degC_evening: String = "NaN",
  barometer_obs_unit:                     String = "NaN",
  barometer_temperature_obs_unit:         String = "NaN")

/**
  * Case class for reading daily air pressure observation data in the year after 1862
  * Missing observations are flagged with 'NaN'.
  * 3 daily obs taken in this order morning, noon, evening
  *
  * @author Honeylal Muraleedharan
  * @version 1.0
  */
case class AirPressureObservationSchema(
  year:                 String,
  month:                String,
  day:                  String,
  air_pressure_morning: String = "NaN",
  air_pressure_noon:    String = "NaN",
  air_pressure_evening: String = "NaN",
  air_pressure_unit:    String = "NaN")