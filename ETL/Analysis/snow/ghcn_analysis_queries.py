import sys


assert sys.version_info >= (3, 5) # make sure we have Python 3.5+
from pyspark.sql import SparkSession, types
from datetime import datetime
from pyspark.sql.functions import date_format, to_date, month, unix_timestamp
def main(inputs, stations_input,output):
    observation_schema = types.StructType([ 
    types.StructField('station', types.StringType()),
    types.StructField('date', types.StringType()),
    types.StructField('observation', types.StringType()),
    types.StructField('value', types.IntegerType()),
    types.StructField('mflag', types.StringType()),
    types.StructField('qflag', types.StringType()),
    types.StructField('sflag', types.StringType()),
    types.StructField('obstime', types.StringType()), ])
    observations = spark.read.parquet(inputs).cache()
    stations = spark.read.parquet(stations_input).cache()
    observations.createOrReplaceTempView("observations")
    stations.createOrReplaceTempView("stations")
    stations.show()
    snow_depth_analysis = spark.sql("SELECT o.station, s.name, o.value \
                                    FROM stations s \
                                    INNER JOIN observations o \
                                    ON s.id  = o.station \
                                    WHERE o.observation = 'SNWD' \
                                    GROUP BY o.station, s.name, o.value \
                                    ORDER BY o.value DESC")
    snow_depth_analysis.createOrReplaceTempView("snow_depth_analysis")
    snow_depth_analysis.show()
if __name__ == '__main__':
    inputs = sys.argv[1]
    stations_input = sys.argv[2]
    output = sys.argv[3]
    spark = SparkSession.builder.appName('temp range').getOrCreate()
    assert spark.version >= '3.0' # make sure we have Spark 3.0+
    spark.sparkContext.setLogLevel('WARN')
    sc = spark.sparkContext
    main(inputs, stations_input, output) 