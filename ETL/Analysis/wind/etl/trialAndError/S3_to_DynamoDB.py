import sys
assert sys.version_info >= (3, 5) # make sure we have Python 3.5+

from pyspark.sql import SparkSession, functions, types

# we don't want low quality data
def filter_data(data):
    return data.where(data.qflag.isNull())
        #.where(data.observation == "WSFG")\
        #.where(data.observation == "WT03")\
        #.where(data.observation == "TAVG")
        #.where(data.station.startswith('CA'))\ # is already the Canadian data

def main(inputs):
    observation_schema = types.StructType([
        types.StructField('station', types.StringType()),
        types.StructField('date', types.StringType()),
        types.StructField('observation', types.StringType()),  # this will be gone! table name
        types.StructField('value', types.IntegerType()),
        types.StructField('mflag', types.StringType()),
        types.StructField('qflag', types.StringType()),
        types.StructField('sflag', types.StringType()),
        types.StructField('obstime', types.StringType()),
    ])
    # first I wanna try it with local csv files rather than S3!
    #raw_data = spark.read\
        #.format("s3selectCSV")\
        #.schema(observation_schema)\
        #.options(compression='gzip')\
        #.load(S3_bucket_path)\

    raw_data = spark.read.csv(inputs, schema=observation_schema)
    weather_data = filter_data(raw_data).cache()

    WSFG = weather_data.where(weather_data.observation == "WSFG")

    # write to some other table overwriting existing item with same keys
    # pyspark --packages com.audienceproject:spark-dynamodb_2.12:1.1.2
    # SPARK_JAVA_OPTS (https://spark.apache.org/docs/0.8.1/configuration.html#system-properties)
    # export SPARK_JAVA_OPTS="-Daws.dynamodb.region=us-west-2 -Daws.dynamodb.endpoint=dynamodb.us-west-2.amazonaws.com"
    WSFG.write.option("tableName", "jeanne-test-table")\
        .format("dynamodb") \
        .save()


if __name__ == '__main__':
    inputs = sys.argv[1]
    spark = SparkSession.builder.appName('S3_to_Dynamo').getOrCreate()
    assert spark.version >= '3.0' # make sure we have Spark 3.0+
    spark.sparkContext.setLogLevel('WARN')
    sc = spark.sparkContext
    main(inputs)