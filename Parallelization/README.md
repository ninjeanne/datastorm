# Bigness and Parallelization

## Cluster configuration
* Software configuration
  * Release: emr-6.8.0
  * Application: Spark
* Hardware configuration
  * Instance type: c7g.large
  * Number of instances:
    * 1 main node
    * 2 worker nodes
* Specifications per instance
  * 4 CPUs/cores
    * 40 threads (~ times 10)
  * 8 GB RAM


## Spark-submit options
* --num-executors: num-cores-per-node * total-nodes-in-cluster = 4 x 3 = 12
* --executor-cores: one executor per core = 1
* --executor-memory: mem-per-node/num-executors-per-node = 8GB/12 = 600M
* --conf spark.dynamicAllocation.enabled=false

## Pricing
TODO

## Partitioning
Original dataset: 
* <TODO_SIZE_IN_GB>
* CSV
* Zipped

Optimize parallelism:
* 12 observations of interest
* 1 Executor reads one file
  * we need equally sized files
* 1 Executor per observation
  * in total 12 observations of interest
  * Observation at least divided into 10 files (number of threads per CPU): 12*10 = 120
* Number of years: 
  * (2021-1900) = 121
* (Number of years)*(observations of interest)*(10) = 14520
  * Round it up to 15000
* use "s3selectCSV" filter

Restrictions:
* The data stored in Parquet files is self-explanatory, meaning it contains metadata that can increase the storage size
* Executors have to open and close too many files at once
* Optimal file size for parquet is 1GB according to the documentation

### Process raw data in EMR
To find the optimal number of partitions, we decided to compare different partition sizes
* no partitioning (dynamic allocation of spark activated)
* 15000 Partitions
* 1500 Partitions

|                   | No partitioning | 1500p   | 15000p  |
|-------------------|-----------------|---------|---------|
| Total size        | 813             | 3.7GB   | 6.7GB   |
| Number of objects | 1.6GB           | 18,265  | 171,688 |