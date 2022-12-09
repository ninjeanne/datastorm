# Extract-Transform-Load (ETL)

Our dataset came from GHCNd. The organization decided to use DLY, where values can be read from fixed positions. We worked out the key for this, as it was not documented for either values or metadata.
We then encountered further difficulties splitting the data series (this comprised a whole month of data) into a more aggregatable format.
A better representation of this was found on our CMPT-732 course's cluster that we used for [these](final/partitioning) scripts, but we have shown [here](final/dly-transformation) how DLY can be converted to the same CSV format of the SFU cluster as well as our final format. 

## Final ETL files
[Script for transforming DLY files](final)

## Data analysis
Our analyses with the data provided in both DLY and CSV helped us derive a more convenient data format. The following compares the properties of both structures of the same data set.

### Properties of the GHCNd dataset
Examples can be found [here](final/dly-transformation)
* The data does not include column names and uses a non-self-explanatory format.
  * We need to add column names and data types
* The values have to be read from fixed positions (for txt- and dly-files)
  * We need to split the values and use a different delimiter
* no streaming service provides us with real-time data
* Raw data contains weather data for the whole world
  * We need to filter the data for Canada
* Raw data is zipped and has to be downloaded from a website
  * Make it accessible, e.g. with S3
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
  * Use a format like Avro or Parquet instead
* It still contains weather data for the whole world over a long period of time
  * Filter on Canada
  * Filter on data after 1900
* A row contains data for only one date and one observation
  * That's better, but we still have to read every row, and aggregating the data takes some time
* A row doesn't contain information on the state
  * We still have to join the dataset with the station's metadata

### Conclusions
* We are not interested in every observation, but a few selected ones that we have stated in our problem
* Structure
  * similar to the CSVs but joined with metadata on stations
* Denormalization
  * joined stations with values
* Accessibility
  * Will be stored in S3, publicly available
* Partitioning
  * after observations accordingly to our future queries
* Format: Parquet
  * Self-explanatory
  * Column-oriented (better for faster serverless queries on S3!)
  * Can be partitioned easily
  * It doesn't need a delimiter
* Compression: Snappy (default value of spark.sql.parquet.compression.codec)
* Filter
  * only Canadian data
  * after 1900
  * Qflag = Null to ensure only high quality data
  * Dropping other irrelevan columns 

## Optimization
After setting up our pipeline from uploading the raw data to finally crawling it, we came to more conclusions in transforming the data more optimally.
We discuss this in our chapter [Visualization](../Visualization)
