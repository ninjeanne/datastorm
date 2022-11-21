import sys

assert sys.version_info >= (3, 5)  # make sure we have Python 3.5+
import subprocess

subprocess.check_call([sys.executable, "-m", "pip", "install", "boto3"])
import boto3
from pyspark.sql import SparkSession, functions, types
from decimal import Decimal

dynamodb = boto3.resource('dynamodb', 'us-west-2')


def batch_write(table_name, rows):
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


def get_station_data():
    stations_data = sc.textFile("../../data.nosync/ghcnd-stations.txt")
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


# TODO: First, create the tables "WSFG", "WT03", "TAVG" in DynamoDB with the
#  primary key (composite key): partition_key station_id and sort_key date
def etl():
    observations_of_interest = ["WSFG", "WT03", "TAVG"]
    data = spark.read.csv("../../data.nosync/cluster-data/2020.csv.gz", schema=get_data_schema())
    cleaned_raw_data = data.where(data["station_id"].startswith("CA"))
    print("Read and clean the raw data")
    cleaned_raw_data.show(5)

    stations = get_station_data()

    # functions.to_date(functions.col("date"), "yyyyMMdd").alias("date") DynamoDB doesn't like the datetype
    merged = stations \
        .join(cleaned_raw_data, 'station_id') \
        .select('station_id', 'station_name', 'state', 'longitude', 'latitude', 'elevation',
                'date', 'observation', 'value') \
        .where(data["qflag"] != "null") \
        .where(functions.col("observation").isin(observations_of_interest)) \
        .cache()

    print("Merged the raw data with its meta data")
    merged.show(5)

    for observation in observations_of_interest:
        observation_table = merged.where(functions.col("observation") == observation).drop("observation")

        print("Start storing the data for observation {}", observation)
        observation_table.show(5)

        batch_write(observation, observation_table.rdd)


if __name__ == '__main__':
    spark = SparkSession.builder.appName('Datastorm').getOrCreate()
    assert spark.version >= '3.0'  # make sure we have Spark 3.0+
    spark.sparkContext.setLogLevel('WARN')
    sc = spark.sparkContext
    etl()
