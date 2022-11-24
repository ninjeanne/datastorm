# Bigness and Parallelization

## Cluster configuration
* Software configuration
  * Release: emr-6.9.0
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
* To find the optimal number of partitions, we decided to compare different partition sizes
  * no partitioning (dynamic allocation of spark activated)
  * 15000 Partitions
  * 1500 Partitions
* Test files
  * data
    * year: 2010
    * size: 197.5MB
    * compression: gzip
    * format: CSV
  * stations
    * includes all stations
    * size: 10.1MB
    * compression: none
    * format: TXT (fixed positions)
* Test Queries in Athena
  * Query 1 
    * select count(observations.observation), observation, date from observations where observations.date LIKE '2010%' GROUP BY observations.observation, observations.date ORDER BY observations.date;
  * Query 2
    * select * from observations where observations.observation = 'TMAX' OR observations.observation = 'PRCP' LIMIT 100;
* other configurations:
  * s3selectCSV-filter activated for the CSV files
  * output will be compressed with Snappy


| Service |                   |              | No partitioning | 1500p     | 15000p    | 12p        |
|---------|-------------------|--------------|-----------------|-----------|-----------|------------|
| EMR     | Elapsed time      |              | 52s             | 3min      | 26min     | 54 seconds |
| S3      | Total size        |              | 14.5MB          | 98.9 MB   | 289.8 MB  | 19.7MB     |
|         | Number of objects |              | 10              | 9,009     | 89,803    | 81         |
| Glue    | Elapsed time      |              | 47s             | 59s       | 1m24s     | 48s        |
| Athena  | Query 1           | Run time     | 1.232s          | 2.42s     | 6.363s    | 1.159 sec  |
|         |                   | Data scanned | 88.99KB         | 8.43 MB   | 14.34 MB  | 2.19 MB    |
|         | Query 2           | Run time     | 869 ms          | 949 ms    | 1.73s     | 749 ms     |
|         |                   | Data scanned | 6.97 MB         | 160.35 KB | 557.64 KB | 776.29 KB  |

