import sys
assert sys.version_info >= (3, 5) # make sure we have Python 3.5+
import boto3
import re
import csv
from pyspark.sql import SparkSession, functions, types

dynamodb = boto3.resource('dynamodb', 'us-west-2')


def batch_write(table_name,rows):
    table = dynamodb.Table(table_name)

    with table.batch_writer() as batch:
        for row in rows:
            item ={
                'Station_ID': row[0],
                'latitude': row[1],
                'longitude': row[2],
                'elevation': row[3],
                'state': row[4],
                'station name': row[5],               
            }
            batch.put_item(item)
    return True

def read_csv(csv_file,list):
    rows = csv.reader(open(csv_file))

    for row in rows:
        list.append(row)



def stations_schema():
    return types.StructType([
        types.StructField("Station ID", types.StringType()),
        types.StructField("Latitude", types.FloatType()),
        types.StructField("Longitude", types.FloatType()),
        types.StructField("Elevation", types.FloatType()),
        types.StructField("State", types.StringType()),
        types.StructField("Station Name", types.StringType()),
        #types.StructField("gsn_flag", types.StringType()),
        #types.StructField("crn_flag", types.StringType()),
        #types.StructField("wmo_id", types.StringType()),
    ])

def parse_line(line):
    id = '(\S+)'
    latitude = '([-+]?(?:\d*\.\d+|\d+))'
    longitude = '([-+]?(?:\d*\.\d+|\d+))'
    elevation = '([-+]?(?:\d*\.\d+|\d+))'
    state = '([-a-zA-Z0-9_][-a-zA-Z0-9_])'
    name = '((\S+\s)+)'
    delimiter = '\s+'
    any = ".*"
    line_re = re.compile(r'^'+ id + delimiter + latitude + delimiter + longitude + delimiter + elevation + delimiter + state + delimiter + name + any + '$')
    splitted_line = re.match(line_re, line)
    return Row(splitted_line.group(1), float(splitted_line.group(2)), float(splitted_line.group(3)), float(splitted_line.group(4)), splitted_line.group(5), splitted_line.group(6))


def etl():
    ghcn_data = 'data/ghcn'

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

    ghcn_df = spark.read.csv(ghcn_data, schema = observation_schema)
    stations_input = sc.textFile("data/ghcnd-stations.txt")
    formatted_lines = stations_input.filter(lambda line: line.startswith("CA")).map(parse_line)
    cleaned_stations = spark.createDataFrame(data=formatted_lines, schema = stations_schema())
    canada_stations_dataframes = cleaned_stations
    canada_stations_dataframes.coalesce(1).write.csv('data/canada_stations.csv')


    


if __name__ == '__main__':
    spark = SparkSession.builder.appName('Reddit Average Dataframes').getOrCreate()
    assert spark.version >= '3.0' # make sure we have Spark 3.0+
    spark.sparkContext.setLogLevel('WARN')
    sc = spark.sparkContext

    table_name = 'test-2'
    file_name = "data/ghcnd-stations.csv"

    items = []

    read_csv(file_name, items)
    status = batch_write(table_name, items)

    if (status):
        print('Data is saved')
    else:
        print('Error while inserting')