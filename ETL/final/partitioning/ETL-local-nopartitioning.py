import sys

assert sys.version_info >= (3, 5)  # make sure we have Python 3.5+
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


def observations_schema():
    return types.StructType([
        types.StructField('station', types.StringType()),
        types.StructField('date', types.StringType()),
        types.StructField('observation', types.StringType()),
        types.StructField('value', types.IntegerType()),
        types.StructField('mflag', types.StringType()),
        types.StructField('qflag', types.StringType()),
        types.StructField('sflag', types.StringType()),
        types.StructField('obstime', types.StringType()),
    ])


def parse_line(line):
    station_id = line[:11]
    latitude = line[11:20]
    longitude = line[20:30]
    elevation = line[30:37]
    state = line[38:40]
    station_name = line[41:72]
    return Row(station_id, float(latitude), float(longitude), float(elevation), state, station_name)


def filter_raw_data(ghcn_df):
    canada_weather_data = ghcn_df.filter(ghcn_df.station.startswith('CA'))
    canada_weather_data = canada_weather_data.drop("sflag", "mflag", "obstime")
    canada_weather_data = canada_weather_data.filter(canada_weather_data['qflag'].isNull()).drop('qflag')
    canada_weather_data = canada_weather_data.filter(
        canada_weather_data['date'].like('19%') | canada_weather_data['date'].like('20%'))
    columns_of_interest = ["PRCP", "SNOW", "WSFG", "WDFG", "MDPR", "SNWD", "WESF", "WESD", "TMAX", "TAVG", "TMIN",
                           "WT03", "WT05", "WT16"]
    return canada_weather_data.filter(canada_weather_data.observation.isin(columns_of_interest))


def etl(inputs, outputs):
    stations_input = sc.textFile(inputs + "ghcnd-stations.txt")
    formatted_lines = stations_input.filter(lambda line: line.startswith("CA")).map(parse_line)
    cleaned_stations = spark.createDataFrame(data=formatted_lines, schema=stations_schema())

    # Since we know that the number of stations in Canada = 9217,
    # we can conclude that this is a small dataframe, hence we use coalesce
    cleaned_stations.coalesce(1).write.option("header", True) \
        .mode("overwrite") \
        .parquet(outputs + "stations")

    ghcn_df = spark.read.csv(inputs, schema=observations_schema())
    canada_weather_data = filter_raw_data(ghcn_df)

    canada_weather_data = canada_weather_data.join(functions.broadcast(cleaned_stations),
                                                   canada_weather_data['station'] == cleaned_stations[
                                                       'Station ID']).drop('Station ID')

    canada_weather_data.write \
        .partitionBy("observation") \
        .parquet(outputs + "observations")


if __name__ == '__main__':
    inputs = sys.argv[1]
    outputs = sys.argv[2]
    spark = SparkSession.builder.appName('Datastorm local - no p').getOrCreate()
    assert spark.version >= '3.0'  # make sure we have Spark 3.0+
    spark.sparkContext.setLogLevel('WARN')
    sc = spark.sparkContext
    etl(inputs, outputs)
