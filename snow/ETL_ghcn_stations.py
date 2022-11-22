'''
Gets info for Canada's weather stations from ghcnd-stations and stores them in parquet file
command to run:  ${SPARK_HOME}/bin/spark-submit ETL_phase1.py ghcnd-stations.txt canada-ghcnd-stations
'''
import sys
assert sys.version_info >= (3, 5) # make sure we have Python 3.5+
from pyspark.sql import SparkSession, functions, types, Row
import re
def parse_line(line):
    country_code = re.match(r'[A-Z]{2,3}[0-9,A-Z]{8,9}',line).group()
    latitude, longitude, elevation = re.findall(r'(\d+\.\d+)',line)[:3]
    state_station_pattern = re.search(r'(\s[A-Z]{2}\s)((\S+\s)+)',line)
    # if state_station_pattern:
    state, station_name = state_station_pattern.group(1), state_station_pattern.group(2)
    state = state_station_pattern.group(1)
    
    return(Row(country_code,float(latitude),float(longitude),float(elevation),state, station_name))
def stations_schema():
    return types.StructType([
        types.StructField("id", types.StringType()),
        types.StructField("latitude", types.FloatType()),
        types.StructField("longitude", types.FloatType()),
        types.StructField("elevation", types.FloatType()),
        types.StructField("state", types.StringType()),
        types.StructField("name", types.StringType()),
    ])
def main(inputs, output):
    split_data = sc.textFile(inputs).filter(lambda line: line.startswith("CA")).map(parse_line)
    df = spark.createDataFrame(data=split_data, schema = stations_schema())
    df.write.parquet(output, mode='overwrite')
    
if __name__ == '__main__':
    inputs = sys.argv[1]
    output = sys.argv[2]
    spark = SparkSession.builder.appName('temp range').getOrCreate()
    assert spark.version >= '3.0' # make sure we have Spark 3.0+
    spark.sparkContext.setLogLevel('WARN')
    sc = spark.sparkContext
    main(inputs, output) 