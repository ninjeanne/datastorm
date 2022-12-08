import sys

assert sys.version_info >= (3, 5)  # make sure we have Python 3.5+

from pyspark.sql import SparkSession, functions
from pyspark.ml import PipelineModel

spark = SparkSession.builder.appName('Observation prediction').getOrCreate()
spark.sparkContext.setLogLevel('WARN')
assert spark.version >= '3.0'


def test_model(path_to_data, observation, path_to_trained_model, output_path):
    print("Read the data, sort it by the date desc and prefilter it for the observation {}", observation)
    # FORMAT
    # station| date|value|latitude|longitude|elevation|state| observation
    # date in yyyy-MM-dd
    # data = spark.read.parquet(path_to_data)

    data = spark.read.parquet(path_to_data) \
        .withColumnRenamed("Latitude", "latitude") \
        .withColumnRenamed("Longitude", "longitude") \
        .withColumnRenamed("Elevation", "elevation") \
        .withColumnRenamed("State", "state") \
        .withColumn("date", functions.date_format(functions.to_date(functions.col("date"), "yyyyMMdd"), "yyyy-MM-dd"))

    data = data.where(functions.col("observation") == observation)\
        .sort(functions.col("date").desc())

    print("Get the latest entries per station and the observation {}", observation)
    latest_date_per_station = data.groupBy(functions.col("station").alias("newest_station"))\
        .agg(functions.first("date").alias("latest_date"))
    latest_entries = data.join(functions.broadcast(latest_date_per_station),
                            (functions.col("station") == functions.col("newest_station")) &
                            (functions.col("date") == functions.col("latest_date")))\
        .drop("newest_station")\
        .drop("latest_date").withColumn("date_with_type", functions.to_date(functions.col("date"), "yyyy-MM-dd"))\
        .cache()

    print("Duplicate the latest entries and add one day to the date")
    new_dates = latest_entries.drop("date")\
        .withColumn('date', functions.date_add(latest_entries['date_with_type'], 1))\
        .drop('date_with_type')
    new_dates.show()

    print("Remove string date column from latest entries' dataframe")
    latest_entries = latest_entries.drop("date").withColumnRenamed("date", "date_with_type")
    latest_entries.show()

    data_with_a_new_day = new_dates.unionAll(latest_entries)
    print("verify the duplication for one station")
    data_with_a_new_day.where(functions.col("station") == "CA003035198").show()

    print("Load the model")
    model = PipelineModel.load(path_to_trained_model)

    print("Start prediction for new dates")
    predictions = model.transform(data_with_a_new_day)
    predictions.show()

    print("Store predictions")
    predictions.write.partitionBy("observation").mode("overwrite").parquet(output_path)


if __name__ == '__main__':
    data_path = sys.argv[1]
    observation = sys.argv[2]
    path_to_trained_model = sys.argv[3]
    output_path = sys.argv[4]
    test_model(data_path, observation, path_to_trained_model, output_path)
