# Explanation
These scripts here were uploaded to Amazon EMR (see [Running](../../../RUNNING.md)) to compare the effects of partitioning on our dataset
(tested with a subset from 2010 and with data from 1900-2021).
The scripts were created as a team, mostly inspired from [here](../../Analysis/precipitation/Final%20EMR%20Scripts).
The cluster configuration and our results can be found under [Parallelization](../../../Parallelization). We used the results for tuning our EMR cluster.

## Usage
EMR
```bash
spark-submit --conf spark.dynamicAllocation.enabled=false --conf spark.yarn.maxAppAttempts=1 --num-executors=12 --executor-cores=1 --executor-memory=600M s3://kpd3-datastorm-cmpt732/<SCRIPT> <INPUT> <OUTPUT>
# example
spark-submit --conf spark.dynamicAllocation.enabled=false --conf spark.yarn.maxAppAttempts=1 --num-executors=12 --executor-cores=1 --executor-memory=600M s3://kpd3-datastorm-cmpt732/ETL_script-emr-s3.py s3://kpd3-datastorm-cmpt732/ s3://kpd3-datastorm-cmpt732/data_after_ETL-nopartitioning/
```
local
```bash
# input contains ghcnd-stations.txt and weather data (for example for a year only the file 2020.csv.gz)
spark-submit ETL/final/partitioning/ETL-local-nopartitioning.py input/ output/
```

## Filtering
### Extracting Only Canada Data Using Station Name
```python
canada_weather_data = ghcn_df.filter(ghcn_df.station.startswith('CA'))
```

### Checking for & Removing Unnecessary Columns
* mflag indicates measurement flags that are not required for this study.
* sflag indicates the source of the data collected which is also not required for this study. 
* obstime indicates the time at which the data was recorded and can be dropped as well.
* qflag being null indicates that the specific data entry did not pass all of the necessary quality checks. In order to ensure that the data we use is of the highest quality, we eliminate any rows that do not satisfy this condition.
```python
canada_weather_data = canada_weather_data.drop("sflag", "mflag", "obstime")
canada_weather_data = canada_weather_data.filter(canada_weather_data['qflag'].isNull()).drop('qflag')
```

### Only taking data from 1900-2021
```python
canada_weather_data = canada_weather_data.filter(canada_weather_data['date'].like('19%') | canada_weather_data['date'].like('20%'))
```

### Columns of interest
Link to the [documentation](https://www1.ncdc.noaa.gov/pub/data/ghcn/daily/readme.txt)
* PRCP = Precipitation (mm or inches as per user preference, inches to hundredths on Daily Form pdf file)
* SNOW = Snowfall (mm) 
* SNWD = Snow depth (mm)
* WESF = Water equivalent of snowfall (tenths of mm)
* WESD = Water equivalent of snow on the ground (tenths of mm)
* WDFG = Direction of peak wind gust (degrees)
* MDPR = Multiday precipitation total (tenths of mm)
* TAVG = Average Temperature
* TMAX = Maximum temperature (Fahrenheit or Celsius as per user preference, Fahrenheit to tenths on Daily Form pdf file
* TMIN = Minimum temperature (Fahrenheit or Celsius as per user preference, Fahrenheit to tenths on Daily Form pdf file
* WT** = Weather Type where ** has one of the following values:
  * WT03 = Thunder
  * WT05 = Hail (may include small hail)
  * WT16 = Rain (may include freezing rain, drizzle, and freezing drizzle)

Further analysis splitted into:
* Precipitation - TMAX, TMIN, PRCP, WT05, WT16 (Karishma)
* Wind - WSFG, WT03, TAVG, WDFG (Jeanne)
* Snow - SNOW, SNWD, WESF, WESD (Crystal)

```python
  columns_of_interest = ["PRCP", "SNOW", "WSFG", "WDFG","MDPR", "SNWD", "WESF", "WESD", "TMAX", "TAVG", "TMIN","WT03","WT05","WT16"]
  canada_weather_data =  canada_weather_data.filter(canada_weather_data.observation.isin(columns_of_interest))
```

### Joining Weather Data with Station Data
```python
canada_weather_data = canada_weather_data.join(functions.broadcast(cleaned_stations),canada_weather_data['station'] == cleaned_stations['Station ID']).drop('Station ID')
```

### Partitioning Of Data Into Required Folders For Ease Of Analysis
```python
NUM_OF_PARTITIONS = 1
canada_weather_data.repartition(NUM_OF_PARTITIONS).write (...)
```
