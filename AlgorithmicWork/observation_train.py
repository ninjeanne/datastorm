import sys
from pyspark.ml import Pipeline
from pyspark.ml.feature import VectorAssembler, SQLTransformer
from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.ml.regression import GBTRegressor
from pyspark.sql import SparkSession, types
assert sys.version_info >= (3, 5)  # make sure we have Python 3.5+


def main(path_to_data, path_to_trained_model):
    data = spark.read.parquet(path_to_data)
    training, validation = data.randomSplit([0.75, 0.25])
    training = training.cache()
    validation = validation.cache()

    assemble_features = VectorAssembler(
        inputCols=['latitude', 'longitude', 'elevation', 'day_of_year', 'yesterdays_value', 'observation'],
        outputCol='features')
    regressor = GBTRegressor(
        featuresCol='features', labelCol='value')
    yesterday_query = "SELECT today.station, today.date, dayofyear(today.date) as day_of_year, today.latitude, " \
                        "today.longitude, today.elevation, yesterday.value AS yesterdays_value, " \
                        "today.value " \
                      "FROM __THIS__ as today " \
                      "INNER JOIN __THIS__ as yesterday " \
                      "ON date_sub(today.date, 1) = yesterday.date " \
                        "AND today.station = yesterday.station " \
                        "AND today.observation = yesterday.observation"
    yesterday_transformer = SQLTransformer(statement=yesterday_query)
    pipeline = Pipeline(stages=[yesterday_transformer, assemble_features, regressor])

    print("Start training the model")
    model = pipeline.fit(training)

    print("Validate the model")
    predictions = model.transform(validation)
    predictions.show()

    print("Evaluate the model")
    r2_evaluator = RegressionEvaluator(predictionCol='prediction', labelCol='value', metricName='r2')
    r2 = r2_evaluator.evaluate(predictions)
    print('R2 =', r2)
    rmse_evaluator = RegressionEvaluator(predictionCol='prediction', labelCol='value', metricName='rmse')
    rmse = rmse_evaluator.evaluate(predictions)
    print('RMSE =', rmse)

    print("Save trained model")
    model.write().overwrite().save(path_to_trained_model)

if __name__ == '__main__':
    path_to_data = sys.argv[1]
    path_to_trained_model = sys.argv[2]
    spark = SparkSession.builder.appName('observation prediction train').getOrCreate()
    assert spark.version >= '3.0'
    spark.sparkContext.setLogLevel('WARN')
    main(path_to_data, path_to_trained_model)
