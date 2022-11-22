All scripts pertaining to ETL for snow related analysis in Canada
## ETL_canada
for 2021
+-----------+------+
|observation| count|
+-----------+------+
|       WESD| 13879|
|       TMIN|239449|
|       MDPR|  2515|
|       WSFG| 95462|
|       DAPR|  2515|
|       TMAX|239268|
|       SNOW|190598|
|       WDFG| 94524|
|       WESF| 31832|
|       SNWD|130912|
|       TAVG|237035|
|       PRCP|393184|
+-----------+------+
WESD = Water equivalent of snow on the ground (tenths of mm)
MDPR = Multiday precipitation total (tenths of mm; use with DAPR and 
	          DWPR, if available)
WSFG = Peak gust wind speed (tenths of meters per second)
DAPR = Number of days included in the multiday precipiation 
	          total (MDPR)
SNOW = Snowfall (mm)
WDFG = Direction of peak wind gust (degrees)
WESF = Water equivalent of snowfall (tenths of mm)
SNWD = Snow depth (mm)
TAVG = Average temperature (tenths of degrees C)
	          [Note that TAVG from source 'S' corresponds
		   to an average for the period ending at
		   2400 UTC rather than local midnight]
PRCP = Precipitation (tenths of mm)

## ETL_ghcn
This script stores all canada records after 1900 in separate parquet files for further analysis.

## ETL_ghcn_stations
Gets info for Canada's weather stations from ghcnd-stations and stores them in parquet file
**command to run**:  ${SPARK_HOME}/bin/spark-submit ETL_phase1.py ghcnd-stations.txt canada-ghcnd-stations
