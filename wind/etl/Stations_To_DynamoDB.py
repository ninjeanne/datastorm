import sys
assert sys.version_info >= (3, 5) # make sure we have Python 3.5+
import re
from pyspark.sql import SparkSession, functions, types, Row
from decimal import Decimal
import sys, subprocess
subprocess.check_call([sys.executable, "-m", "pip", "install", "boto3"])
import boto3

dynamodb = boto3.resource('dynamodb', 'us-west-2')


def batch_write(table_name, rows):
    table = dynamodb.Table(table_name)

    with table.batch_writer() as batch:
        for row in rows.collect():
            item = {
                'station_id': row[0] ,
                'latitude': Decimal(row[1]),
                'longitude': Decimal(row[2]),
                'elevation': Decimal(row[3]),
                'state': row[4],
                'station_name': row[5],
            }
            batch.put_item(item)
    return True



def stations_schema():
    return types.StructType([
        types.StructField("id", types.StringType()),
        types.StructField("latitude", types.FloatType()),
        types.StructField("longitude", types.FloatType()),
        types.StructField("elevation", types.FloatType()),
        types.StructField("state", types.StringType()),
        types.StructField("name", types.StringType()),
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
    ghcn_data = '../../data.nosync/'
    stations_input = sc.textFile(ghcn_data + "ghcnd-stations.txt")
    formatted_lines = stations_input.filter(lambda line: line.startswith("CA")).map(parse_line)
    cleaned_stations = spark.createDataFrame(data=formatted_lines, schema = stations_schema())
    cleaned_stations.show()
    canada_stations_rdd = cleaned_stations.rdd
    batch_write(table_name, canada_stations_rdd)


if __name__ == '__main__':
    table_name = 'jeanne-station-test'
    spark = SparkSession.builder.appName('Datastorm').getOrCreate()
    assert spark.version >= '3.0' # make sure we have Spark 3.0+
    spark.sparkContext.setLogLevel('WARN')
    sc = spark.sparkContext
    etl()