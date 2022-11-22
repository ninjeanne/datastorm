'''
This script stores all canada records after 1900 in separate parquet files for further analysis.

'''
import sys
assert sys.version_info >= (3, 5) # make sure we have Python 3.5+
from pyspark.sql import SparkSession,  types
from pyspark.sql.functions import substring

def main(inputs, output):
    observation_schema = types.StructType([ 
    types.StructField('station', types.StringType()),
    types.StructField('date', types.StringType()),
    types.StructField('observation', types.StringType()),
    types.StructField('value', types.IntegerType()),
    types.StructField('mflag', types.StringType()),
    types.StructField('qflag', types.StringType()),
    types.StructField('sflag', types.StringType()),
    types.StructField('obstime', types.StringType()), ])
    weather = spark.read.csv(inputs, schema=observation_schema)
    canada_weather = weather.filter(weather.station.startswith('CA')).cache()
    # print(canada_weather.show())
    canada_weather = canada_weather.withColumn("year",substring("date",1,4))
    # print(canada_weather.show())
    canada_weather.write.partitionBy('year').parquet(output)
if __name__ == '__main__':
    inputs = sys.argv[1]
    output = sys.argv[2]
    spark = SparkSession.builder.appName('temp range').getOrCreate()
    assert spark.version >= '3.0' # make sure we have Spark 3.0+
    spark.sparkContext.setLogLevel('WARN')
    sc = spark.sparkContext
    main(inputs, output) 