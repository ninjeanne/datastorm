# Extract-Transform-Load (ETL)

## Final ETL files
[Script for transforming DLY files](final)

## Data analysis
### Properties of the GHCNd dataset
* The data does not include column names and uses a non-self-explanatory format.
  * We need to add column names and data types
* The values have to be read from fixed positions (for txt- and dly-files)
  * We need to split the values and use a different delimiter
* no streaming service provides us with real-time data
* Raw data contains weather data for the whole world
  * We need to filter the data for Canada
* Raw data is zipped and has to be downloaded from a website
  * Now accessible and crawlable in S3
* Raw data contains measurements that don't fulfill our quality standards (QFLAG)
  * Filter data that has a quality issue
* Raw data is partitioned into stations and is sorted by year and month
  * Only some observations contain data for every day.
  * One file is vast in its size, so we have to partition it better
* One row in the raw data contains values for one month per observation.
  * We need to read every row to receive one value. No filtering after a date is possible. We can only aggregate multiple observations with effort.
  * We merge the day to the date column.
  * We denormalize the data
    * Should lead to faster queries and less computation time

### Properties of the CSV dataset on the cluster
* Partitioned into large CSV files and grouped per year
  * We need to partition it better to benefit from Spark's parallelism
* Better delimiter as with the original DLY files, but still not self-explanatory and without datatypes
  * Use Parquet instead
* It still contains weather data for the whole world
  * Filter for Canada
* A row contains data for only one date and one observation
  * That's better, but we still have to read every row, and aggregating the data takes some time
* A row doesn't contain information on the state
  * We still have to join the dataset with the station's metadata
* One data entry has already been joined with the station's metadata (denormalized data, repeating coordinates for a station)

### Other decisions that we've made:
* We are not interested in every observation, but a few selected ones that we have stated in our problem
* Partitioning after observations accordingly to our future queries
* Using Parquet as the new format
  * Self-explanatory
  * Column-oriented (better for serverless queries on S3!)
  * Can be partitioned easily
  * It doesn't need a delimiter
* Compression:
  * spark.sql.parquet.compression.codec 
  * Default: snappy

### Data formats
Examples can be found [here](final/pre-transformation)
