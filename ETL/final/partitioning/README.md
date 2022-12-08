# Explanation
These scripts here were uploaded to Amazon EMR to compare the effects of partitioning on our dataset (tested with a subset from 2010).
The scripts were created as a team, mostly inspired from [here](../../Analysis/precipitation/Final%20EMR%20Scripts)

The cluster configuration and our results can be found under [Parallelization](../../../Parallelization). We used the results for tuning our EMR cluster.

## Usage
```bash
spark-submit --conf spark.dynamicAllocation.enabled=false --conf spark.yarn.maxAppAttempts=1 --num-executors=12 --executor-cores=1 --executor-memory=600M s3://kpd3-datastorm-cmpt732/<SCRIPT> <INPUT> <OUTPUT>
# example
spark-submit --conf spark.dynamicAllocation.enabled=false --conf spark.yarn.maxAppAttempts=1 --num-executors=12 --executor-cores=1 --executor-memory=600M s3://kpd3-datastorm-cmpt732/ETL_script-emr-s3.py s3://kpd3-datastorm-cmpt732/ s3://kpd3-datastorm-cmpt732/data_after_ETL-nopartitioning/
```