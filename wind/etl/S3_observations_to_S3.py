# USAGE: spark-submit s3_observations_to_dynamo_db.py <s3_path_stations_txt> <s3_path_data_folder>
#
# First, create the tables "WSFG", "WT03", "TAVG" in DynamoDB with the
#  primary key (composite key): partition_key station_id and sort_key date

import subprocess
import sys

import boto3

assert sys.version_info >= (3, 5)
from pyspark.sql import SparkSession, types, functions
from decimal import Decimal

subprocess.check_call([sys.executable, "-m", "pip", "install", "boto3"])


def batch_write(table_name, rows):
    dynamodb = boto3.resource('dynamodb', 'us-west-2')
    table = dynamodb.Table(table_name)

    with table.batch_writer() as batch:
        for row in rows.collect():
            item = {
                'station_id': row[0],
                'station_name': row[1],
                'state': row[2],
                'longitude': Decimal(row[3]),
                'latitude': Decimal(row[4]),
                'elevation': Decimal(row[5]),
                'date': row[6],
                'value': Decimal(row[7]),
            }

            batch.put_item(item)
    return True


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

    data = spark.read.format("s3selectCSV")\
        .schema(get_data_schema())\
        .options(compression='gzip')\
        .load(s3_data_bucket)

    print("Read and clean the raw data")
    cleaned_raw_data = data.where(data["station_id"].startswith("CA"))
    cleaned_raw_data.show(5)

    print("Merge the raw data with its meta data")
    merged = stations \
        .join(cleaned_raw_data, 'station_id') \
        .select('station_id', 'station_name', 'state', 'longitude', 'latitude', 'elevation',
                'date', 'observation', 'value') \
        .where(data["qflag"] != "null") \
        .cache()
    merged.show(5)

    merged.write.partitionBy("observation").parquet(s3_output_bucket, mode="overwrite")

if __name__ == '__main__':
    s3_path_stations_txt = sys.argv[1]
    s3_data_bucket = sys.argv[2]
    s3_output_bucket = sys.argv[3]
    spark = SparkSession.builder.appName('s3_observations_to_s3').getOrCreate()
    assert spark.version >= '3.0'
    spark.sparkContext.setLogLevel('WARN')
    sc = spark.sparkContext
    etl(s3_path_stations_txt, s3_data_bucket, s3_output_bucket)
