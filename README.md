## Stockholm Historical Weather Dataload

## Introduction

The aim of this project is to load Stockholm historical weather observation data into Hive tables for further processing (data need to be cleansed before loading into these Hive tables).
The weather observations includes 
Air temperature observations and Air pressure observations. 

## Weather data description

### Air pressure observation (barometer readings) data
Historical air pressure observation data from the Stockholm are stored in 7 files. 
- stockholm_barometer_1756_1858.txt
- stockholm_barometer_1859_1861.txt
- stockholm_barometer_1862_1937.txt
- stockholm_barometer_1938_1960.txt
- stockholm_barometer_1961_2012.txt
- stockholm_barometer_2013_2017.txt
- stockholmA_barometer_2013_2017.txt

URL to download the data https://bolin.su.se/data/stockholm/barometer_readings_in_original_units.php     

### Air temperature observation data
Historical air temperature observation data from the Stockholm are stored in 5 files. 
- stockholm_daily_temp_obs_1756_1858_t1t2t3.txt
- stockholm_daily_temp_obs_1859_1960_t1t2t3txtn.txt
- stockholm_daily_temp_obs_1961_2012_t1t2t3txtntm.txt
- stockholm_daily_temp_obs_2013_2017_t1t2t3txtntm.txt
- stockholmA_daily_temp_obs_2013_2017_t1t2t3txtntm.txt

URL to download the data : https://bolin.su.se/data/stockholm/raw_individual_temperature_observations.php     

### Assumptions
- The data files are available in HDFS.
- Each data file is a plain ascii file with one day per row.
 - All missing observations are flagged with 'NaN'
- 3 daily observations are considered as morning, noon, evening in the given order.
 - From 2013 on-words each day we have readings from manual station and automatic stations. 
	 - To store this we have added an extra column "weather_station_type".
	 - The data prior to 2013 "weather_station_type" column value is set to "NaN".
- Columns 1-3 define the date, starting with 1756 01 01.

	**Note:** *If ***year*** or ***month*** or ***day*** column is null or empty we need to remove the row before storing to Hive tables.*	
- Tables are partitioned by "year" and "month" column values. 

## Application property file description
Application property file is stored in app_home/src/main/resource/ with name "WeatherDataload.properties"

### Property file description 

HDFS location of air temperature observation data
- air.temperature.observation.1756_1858.file.input.path=`path to stockholm_daily_temp_obs_1756_1858_t1t2t3.txt file in HDFS`
- air.temperature.observation.1859_1960.file.input.path=`path to stockholm_daily_temp_obs_1859_1960_t1t2t3txtn.txt file in HDFS`
- air.temperature.observation.1961_2012.file.input.path=`path to sstockholm_daily_temp_obs_1961_2012_t1t2t3txtntm.txt file in HDFS`
- air.temperature.observation.manual.2013_2017.file.input.path=`path to sstockholm_daily_temp_obs_2013_2017_t1t2t3txtntm.txt file in HDFS`
- air.temperature.observation.automatic.1961_2012.file.input.path=`path to stockholmA_daily_temp_obs_2013_2017_t1t2t3txtntm.txt file in HDFS`

HDFS location air temperature observation data table (Hive external table location)
- air.temperature.observation.hive.location=`HDFS path where Hive table store processed air temperature observation data`

HDFS location of air pressure observation data
- air.pressure.observation.1756_1858.file.input.path=`path to stockholm_barometer_1756_1858.txt in HDFS`
- air.pressure.observation.1859_1861.file.input.path=`path to stockholm_barometer_1859_1861.txt in HDFS`
- air.pressure.observation.1862_1937.file.input.path=`path to stockholm_barometer_1862_1937.txt in HDFS`
- air.pressure.observation.1938_1960.file.input.path=`path to raw/stockholm_barometer_1938_1960.txt in HDFS`
- air.pressure.observation.1961_2012.file.input.path=`path to stockholm_barometer_1961_2012.txt in HDFS`
- air.pressure.observation.manual.2013_2017.file.input.path=`path to stockholm_barometer_2013_2017.txt in HDFS`
- air.pressure.observation.automatic.1961_2012.file.input.path=`path to stockholmA_barometer_2013_2017.txt in HDFS`

HDFS location air pressure observation data table (Hive external table location)
- air.pressure.observation.hive.location=`HDFS path where Hive table store processed air pressure observation data`

## How to build the application
### Software required  
	Java 1.8
	Maven 
	 
This application is built using  [Apache Maven](https://maven.apache.org/). 
To build 
 1. Download the code from the GitHub to one local folder (app_home folder).
 2. Open cmd / shell -> goto app_home folder  run below command 
 `mvn clean install`

## How to run the application 

1) Login to cluster edge node and navigate to the app_home directory path.
2) Place the Jar(StockholmWeatherDataload-1.0-jar-with-dependencies.jar) created after maven build to app_home path 
3) Execute the cmd : 
`spark-submit --class org.tcs.spark.weather.dataload.loader.StockholmWeatherDataloader  --master yarn StockholmWeatherDataload-1.0-jar-with-dependencies.jar`

