# USAGE: spark-submit s3_observations_to_S3.py <s3_path_stations_txt> <s3_data_bucket> <s3_output_bucket>
import sys

assert sys.version_info >= (3, 5)
from pyspark.sql import SparkSession, types


def parse_stations_line(line):
    station_id = line[:11]
    latitude = line[11:20]
    longitude = line[20:30]
    elevation = line[30:37]
    state = line[38:40]
    station_name = line[41:72]
    return [station_id, latitude, longitude, elevation, state, station_name]


def get_stations_columns():
    return ["station_id", "latitude", "longitude", "elevation", "state", "station_name"]


def get_station_data(s3_path_stations_txt):
    stations_data = sc.textFile(s3_path_stations_txt)
    formatted_lines = stations_data.filter(lambda line: line.startswith("CA")).map(parse_stations_line)
    cleaned_stations = formatted_lines.toDF(get_stations_columns())
    print("Read and clean the station meta data")
    cleaned_stations.show(5)
    return cleaned_stations


def get_data_schema():
    return types.StructType([
        types.StructField('station_id', types.StringType()),
        types.StructField('date', types.StringType()),
        types.StructField('observation', types.StringType()),
        types.StructField('value', types.IntegerType()),
        types.StructField('mflag', types.StringType()),
        types.StructField('qflag', types.StringType()),
        types.StructField('sflag', types.StringType()),
    ])


def etl(s3_path_stations_txt, s3_data_bucket, s3_output_bucket):
    stations = get_station_data(s3_path_stations_txt)

    data = spark.read.csv(s3_data_bucket, schema=get_data_schema())

    print("Read and clean the raw data")
    cleaned_raw_data = data.where(data["station_id"].startswith("CA"))
    cleaned_raw_data.show(5)

    print("Merge the raw data with its meta data")
    merged = stations \
        .join(cleaned_raw_data, 'station_id') \
        .select('station_id', 'station_name', 'state', 'longitude', 'latitude', 'elevation',
                'date', 'observation', 'value') \
        .where(data["qflag"] != "null")
    merged.show(5)

    print("Start writing to S3 bucket")
    merged.repartition(1500).write.partitionBy("observation").parquet(s3_output_bucket, mode="overwrite")

if __name__ == '__main__':
    s3_path_stations_txt = sys.argv[1]
    s3_data_bucket = sys.argv[2]
    s3_output_bucket = sys.argv[3]
    spark = SparkSession.builder.appName('s3_observations_to_s3').getOrCreate()
    assert spark.version >= '3.0'
    spark.sparkContext.setLogLevel('WARN')
    sc = spark.sparkContext
    etl(s3_path_stations_txt, s3_data_bucket, s3_output_bucket)
