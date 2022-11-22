'''
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
'''
import sys


assert sys.version_info >= (3, 5) # make sure we have Python 3.5+
from pyspark.sql import SparkSession, functions, types

def main(inputs, output):
    observation_schema = types.StructType([ 
    types.StructField('station', types.StringType()),
    types.StructField('date', types.StringType()),
    types.StructField('observation', types.StringType()),
    types.StructField('value', types.IntegerType()),
    types.StructField('mflag', types.StringType()),
    types.StructField('qflag', types.StringType()),
    types.StructField('sflag', types.StringType()),
    types.StructField('obstime', types.StringType()), ])
    weather = spark.read.parquet('output/year=2021').cache()
    # weather = spark.read.csv(inputs, schema=observation_schema)
    weather = weather.filter(weather["qflag"].isNull())
    # weather = weather.groupBy(weather["observation"]).count()
    weather = weather.groupBy(weather["observation"], weather["station"]).count()
    weather = weather.orderBy(weather["station"])
    print(weather.show())
if __name__ == '__main__':
    inputs = sys.argv[1]
    output = sys.argv[2]
    spark = SparkSession.builder.appName('temp range').getOrCreate()
    assert spark.version >= '3.0' # make sure we have Spark 3.0+
    spark.sparkContext.setLogLevel('WARN')
    sc = spark.sparkContext
    main(inputs, output) 