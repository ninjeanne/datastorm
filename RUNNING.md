# Run and test the project

How to get the data is explained [here](Acquiring)
## Transform GHCNd's DLY-files (exemplified with one station)
```bash
spark-submit ETL/final/dly-transformation/ETL-dly-files.py ghcnd-stations.txt CA1AB000001.dly
```

## Run the ETL on an AWS EMR cluster
Our ETL scripts can be found [here](ETL/final/partitioning).
### Cluster configuration
* Software configuration
  * Release: emr-6.9.0
  * Application: Spark
* Hardware configuration
  * Instance type: c7g.xlarge
  * Number of instances:
    * 1 main node
    * 2 worker nodes 
### Add Step
* Spark config
```bash
--conf spark.dynamicAllocation.enabled=false --conf spark.yarn.maxAppAttempts=1 --num-executors=12 --executor-cores=1 --executor-memory=600M
```
* Location of script
```bash
s3://kpd3-datastorm-cmpt732/ETL_script-emr-s3.py
```
* Arguments
```bash
s3://kpd3-datastorm-cmpt732/ 
s3://kpd3-datastorm-cmpt732/data_after_ETL-nopartitioning/
```
* Example
```bash
spark-submit --conf spark.dynamicAllocation.enabled=false --conf spark.yarn.maxAppAttempts=1 --num-executors=12 --executor-cores=1 --executor-memory=600M s3://kpd3-datastorm-cmpt732/ETL_script-emr-s3.py s3://kpd3-datastorm-cmpt732/ s3://kpd3-datastorm-cmpt732/data_after_ETL-nopartitioning/
```
### Local version
```bash
# input contains ghcnd-stations.txt and weather data (for example for a year only the file 2020.csv.gz)
spark-submit ETL/final/partitioning/ETL-local-nopartitioning.py input/ output/
```

## Athena Queries
* Go to Amazon Athena
* Select the Query Editor
* Data source: AwsDataCatalog
* Database: datastorm

### Example
```sql
select distinct(observation) from observations;
```
Our queries can be found [here](AlgorithmicWork). We used Athena with Quicksight's Query Editor.

## QuickSight
* Go to QuickSight
* Go to Shared Folders (menu on the left side)
* Go to datastorm
* There are three icons:
  * Analyses (blue) and Dashboards (green)
    * our final results, stated [here](Visualization)
  * Data sources (Athena's icon, orange)
    * Queries can be accessed by clicking on...
    * datasource > Edit Dataset > Clicking on data (and an dropdown arrow) > Edit SQL Query
    * ... Quicksights Query Editor opens

![shared resources icons](Visualization/shared_resources_icons.png)

## Additional Work
### Testing weather prediction
See [Algorithmic Work](AlgorithmicWork)
* Train model
```bash
# output=path to an older dataset. for example from the previous transformation with an additional filter for the time range
# prediction-model = path for the model's output
spark-submit observation_train.py output/ prediction-model
```
* Test model
```bash
# output = newer dataset. for example from the previous transformation with an additional filter on the past year
# PRCP = example for an specific observation
# prediction-model = name of the trained model output
# results=path for storing the results
spark-submit observation_prediction.py output/ PRCP prediction-model results/
```