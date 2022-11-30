import sys
assert sys.version_info >= (3, 5)
import re
from pyspark.sql import SparkSession, functions, types, Row
import sys



def stations_schema():
    return types.StructType([
        types.StructField("Station ID", types.StringType()),
        types.StructField("Latitude", types.FloatType()),
        types.StructField("Longitude", types.FloatType()),
        types.StructField("Elevation", types.FloatType()),
        types.StructField("State", types.StringType()),
        types.StructField("Station Name", types.StringType())
    ])

def parse_line(line):
    station_id = line[:11]
    latitude = line[11:20]
    longitude = line[20:30]
    elevation = line[30:37]
    state = line[38:40]
    station_name = line[41:72]
    return Row(station_id, float(latitude), float(longitude), float(elevation), state, station_name)


def etl(inputs, outputs):
    ghcn_stations =  'data/stations/'
    stations_input = sc.textFile(inputs + ghcn_stations + "ghcnd-stations.txt")
    formatted_lines = stations_input.filter(lambda line: line.startswith("CA")).map(parse_line)
    cleaned_stations = spark.createDataFrame(data=formatted_lines, schema = stations_schema())

    # Since we know that the number of stations in Canada = 9217, we can conclude that this is a small dataframe, hence we use coalesce
    cleaned_stations.coalesce(1).write.option("header",True) \
        .mode("overwrite") \
        .parquet(outputs + "stations")

    ghcn_data = 'data/ghcn/'

    observation_schema = types.StructType([
    types.StructField('station', types.StringType()),
    types.StructField('date', types.StringType()),
    types.StructField('observation', types.StringType()),
    types.StructField('value', types.IntegerType()),
    types.StructField('mflag', types.StringType()),
    types.StructField('qflag', types.StringType()),
    types.StructField('sflag', types.StringType()),
    types.StructField('obstime', types.StringType()),
    ])


    ghcn_df = spark.read.format("s3selectCSV").schema(observation_schema).options(compression='gzip').load(inputs + ghcn_data+ "2010.csv.gz")

    #### 2.1 Extracting Only Canada Data Using Station Name
    canada_weather_data = ghcn_df.filter(ghcn_df.station.startswith('CA'))

    ####     2.2 Checking for & Removing Unnecessary Columns
    # mflag indicates measurement flags that are not required for this study.
    # sflag indicates the source of the data collected which is also not required for this study. 
    # obstime indicates the time at which the data was recorded and can be dropped as well.
    # qflag being null indicates that the specific data entry did not pass all of the necessary quality checks. In order to ensure that the data we use is of the highest quality, we eliminate any rows that do not satisfy this condition.
    canada_weather_data = canada_weather_data.drop("sflag", "mflag", "obstime")
    canada_weather_data = canada_weather_data.filter(canada_weather_data['qflag'].isNull()).drop('qflag')
    # Only taking data from 1900-2021
    canada_weather_data = canada_weather_data.filter(canada_weather_data['date'].like('19%') | canada_weather_data['date'].like('20%'))
    ### Columns of Interest: 
    # . PRCP = Precipitation (mm or inches as per user preference, inches to hundredths on Daily Form pdf file)
    # . SNOW = Snowfall (mm) 
    # . SNWD = Snow depth (mm)
    # . WESF = Water equivalent of snowfall (tenths of mm)
    # . WESD = Water equivalent of snow on the ground (tenths of mm)
    # . WDFG = Direction of peak wind gust (degrees)
    # . MDPR = Multiday precipitation total (tenths of mm)
    # . TAVG = Average Temperature
    # . TMAX = Maximum temperature (Fahrenheit or Celsius as per user preference, Fahrenheit to tenths on Daily Form pdf file
    # . TMIN = Minimum temperature (Fahrenheit or Celsius as per user preference, Fahrenheit to tenths on Daily Form pdf file
    # . WT** = Weather Type where ** has one of the following values:
    #     . WT03 = Thunder
    #     . WT05 = Hail (may include small hail)
    #     . WT16 = Rain (may include freezing rain, drizzle, and freezing drizzle)
    # Karishma: TMAX, TMIN, PRCP, WT05, WT16
    # Jeanne: WSFG, WT03, TAVG, WDFG
    # Crystal: SNOW, SNWD, WESF, WESD
    columns_of_interest = ["PRCP", "SNOW", "WSFG", "WDFG","MDPR", "SNWD", "WESF", "WESD", "TMAX", "TAVG", "TMIN","WT03","WT05","WT16"]
    canada_weather_data =  canada_weather_data.filter(canada_weather_data.observation.isin(columns_of_interest))

    ### Joining Weather Data with Station Data
    canada_weather_data = canada_weather_data.join(functions.broadcast(cleaned_stations),canada_weather_data['station'] == cleaned_stations['Station ID']).drop('Station ID')

    ### Partitioning Of Data Into Required Folders For Ease Of Analysis
    canada_weather_data.write \
        .partitionBy("observation") \
        .parquet(outputs + "observations")

if __name__ == '__main__':
    inputs = sys.argv[1]
    outputs = sys.argv[2]
    spark = SparkSession.builder.appName('Datastorm no partitioning').getOrCreate()
    assert spark.version >= '3.0' # make sure we have Spark 3.0+
    spark.sparkContext.setLogLevel('WARN')
    sc = spark.sparkContext
    etl(inputs, outputs)

    #  --conf spark.dynamicAllocation.enabled=false --conf spark.yarn.maxAppAttempts=1 --num-executors=12 --executor-cores=1 --executor-memory=600M
    # s3://datastorm-data/ETL-emr-s3-nopartitioning.py
    # s3://kpd3-datastorm-cmpt732/ s3://datastorm-data/data_after_ETL-nopartitioning/