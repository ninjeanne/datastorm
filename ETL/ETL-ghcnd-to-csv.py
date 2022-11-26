import sys
assert sys.version_info >= (3, 5) # make sure we have Python 3.5+
from pyspark.sql import SparkSession, functions, types, Row


def stations_schema():
    return types.StructType([
        types.StructField("station", types.StringType()),
        types.StructField("latitude", types.FloatType()),
        types.StructField("longitude", types.FloatType()),
        types.StructField("elevation", types.FloatType()),
        types.StructField("state", types.StringType()),
        types.StructField("station_name", types.StringType()),
        #types.StructField("gsn_flag", types.StringType()),
        #types.StructField("crn_flag", types.StringType()),
        #types.StructField("wmo_id", types.StringType()),
    ])

def parse_line(line):
    station = line[:11]
    latitude = line[11:20]
    longitude = line[20:30]
    elevation = line[30:37]
    state = line[38:40]
    station_name = line[41:72]
    return Row(station, float(latitude), float(longitude), float(elevation), state, station_name)


def parse_data(line):
    station = line[:11]
    year = line[11:15]
    month = line[15:17]
    element = line[17:21]
    start_index=21
    data = [station, year, month, element]
    for i in range(31):
        value_i = line[start_index:start_index+5]
        mflag_i = line[start_index+5:start_index+6]
        qflag_i = line[start_index+6:start_index+7]
        sflag_i = line[start_index+7:start_index+8]
        start_index=start_index+8
        data.append(int(value_i))
        data.append(mflag_i)
        data.append(qflag_i)
        data.append(sflag_i)

    return data


def ghcnd_all_columns():
    columns = ["station", "year", "month", "element"]
    for i in range(31):
        value_i = "value" + str(i+1)
        mflag_i = "mflag" + str(i+1)
        qflag_i = "qflag" + str(i+1)
        sflag_i = "sflag" + str(i+1)
        columns.append(value_i)
        columns.append(mflag_i)
        columns.append(qflag_i)
        columns.append(sflag_i)

    return columns


def etl(stations_path, data_path):
    stations_input = sc.textFile(stations_path)
    formatted_lines = stations_input.filter(lambda line: line.startswith("CA")).map(parse_line)
    cleaned_stations = spark.createDataFrame(data=formatted_lines, schema=stations_schema())
    cleaned_stations.show()

    ghcnd_all_input = sc.textFile(data_path)
    formatted_lines = ghcnd_all_input.map(parse_data)
    cleaned_ghcnd_all = formatted_lines.toDF(ghcnd_all_columns())
    cleaned_ghcnd_all.show()
    # TODO merge year, month and date, add more lines
    # TODO join with stations.txt
    # TODO output


if __name__ == '__main__':
    stations_path = sys.argv[1]
    data_path = sys.argv[2]
    spark = SparkSession.builder.appName('Transform ghcnd and stations').getOrCreate()
    assert spark.version >= '3.0'
    spark.sparkContext.setLogLevel('WARN')
    sc = spark.sparkContext
    etl(stations_path, data_path)
