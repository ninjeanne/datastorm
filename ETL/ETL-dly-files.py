# sample execution:
#
# spark-submit ETL-dly-files.py ../data.nosync/ghcnd-stations.txt ../data.nosync/ghcnd-all-canada/CA1AB000001.dly
import sys
assert sys.version_info >= (3, 5)
from pyspark.sql import SparkSession, types, Row
import datetime


def stations_schema():
    return types.StructType([
        types.StructField("station", types.StringType()),
        types.StructField("latitude", types.FloatType()),
        types.StructField("longitude", types.FloatType()),
        types.StructField("elevation", types.FloatType()),
        types.StructField("state", types.StringType()),
        types.StructField("station_name", types.StringType()),
    ])

def parse_line(line):
    station = line[:11]
    latitude = float(line[11:20])
    longitude = line[20:30]
    elevation = line[30:37]
    state = line[38:40]
    station_name = line[41:72]
    return Row(station, float(latitude), float(longitude), float(elevation), state, station_name)


def parse_data(line):
    station = line[:11]
    year = line[11:15]
    month = line[15:17]
    observation = line[17:21]
    start_index = 21
    for i in range(31):
        date = datetime.datetime(int(year), int(month), i+1).strftime('%Y%m%d')
        value = float(line[start_index:start_index+5])
        mflag = line[start_index+5:start_index+6]
        qflag = line[start_index+6:start_index+7]
        sflag = line[start_index+7:start_index+8]
        start_index = start_index+8
        if value == -9999:
            # skip values that weren't recorded
            continue

        if qflag == ' ':
            qflag = 'null'

        if mflag == ' ':
            mflag = 'null'

        if sflag == ' ':
            sflag = 'null'

        row = [station, date, observation, value, mflag, qflag, sflag]
        return row


def ghcnd_all_columns():
    return ["station", "date", "observation", "value", "mflag", "qflag", "sflag"]


def etl(stations_path, data_path):
    stations_input = sc.textFile(stations_path)
    formatted_lines = stations_input.map(parse_line)
    cleaned_stations = spark.createDataFrame(data=formatted_lines, schema=stations_schema())
    cleaned_stations.show()  # station|latitude|longitude|elevation|state|station_name

    ghcnd_all_input = sc.textFile(data_path)
    formatted_lines = ghcnd_all_input.map(parse_data)
    cleaned_ghcnd_all = formatted_lines.toDF(ghcnd_all_columns())
    cleaned_ghcnd_all.show()  # station|date|observation|value|mflag|qflag|sflag

    joined_ghcnd_and_stations = cleaned_stations.join(cleaned_ghcnd_all, "station")
    joined_ghcnd_and_stations.show()  # station|latitude|longitude|elevation|state|station_name|date|observation|value|mflag|qflag|sflag|

    # TODO output?


if __name__ == '__main__':
    stations_path = sys.argv[1]
    data_path = sys.argv[2]
    spark = SparkSession.builder.appName('Transform dly-formatted ghcnd-data and stations').getOrCreate()
    assert spark.version >= '3.0'
    spark.sparkContext.setLogLevel('WARN')
    sc = spark.sparkContext
    etl(stations_path, data_path)
