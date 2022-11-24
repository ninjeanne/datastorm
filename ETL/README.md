# Extract-Transform-Load (ETL)

## Data analysis
### Properties of the GHCNd dataset
* The data does not include column names and uses a non-self-explanatory format.
  * We need to add column names and data types
* The values have to be read from fixed positions (for txt- and dly-files)
  * We need to split the values and use a different delimiter
* There is no streaming service that provides us with real-time data
* Raw data contains weather data for the whole world
  * We need to filter the data for Canada
* Raw data is zipped and has to be downloaded from a website
  * Now accessible and crawlable in S3
* Raw data contains measurements that don’t fulfill our quality standards (QFLAG)
  * Filter data that has a quality issue
* Raw data is partitioned into stations and is sorted by year and month
  * Not every observation contains data for every day.
  * One file is vast in its size, so we have to partition it better
* One row in the raw data contains values for one month per observation.
  * We need to read every row to receive one value. No filtering after a date is possible. We can’t aggregate multiple observations without any effort.
  * We merge the day to the date column.
  * We denormalize the data
    * TODO advantages 

### Properties of the CSV dataset on the cluster
* Partitioned into large CSV files and grouped per year
  * We need to partition it better to benefit from Spark’s parallelism
* Better delimiter as with the original DLY files, but still not self-explanatory and without datatypes
  * Use Parquet instead
* It still contains weather data for the whole world
  * Filter for Canada
* A row contains data for only one date and one observation
  * That’s better, but we still have to read every row, and aggregating the data takes some time
* A row  doesn’t contain information on the state
  * We still have to join the dataset with the station’s metadata
* One data entry has already been joined with the station’s metadata (denormalized data, repeating coordinates for a station)

### Other decisions that we’ve made:
* We are not interested in every observation, but a few selected ones that we have stated in our problem
* Partitioning after observations accordingly to our future queries
* Using parquet as the new format
  * Self-explanatory
  * Column-oriented (better for serverless queries on S3!)
  * Can be partitioned easily
  * It doesn't need a delimiter
  * More benefits: https://www.databricks.com/glossary/what-is-parquet

## Data formats
### Metadata (Stations)
* Columns
Station ID	 Latitude    Longitude Elevation State Name GSN Flag CRN Flag WMO ID
* Example 
AE000041196  25.3330   55.5170   34.0    SHARJAH INTER. AIRP            GSN     41196
CA008403619  46.9167  -55.3833   49.0 NL ST LAWRENCE                            71110 

### Raw data
* Columns
station|year|month|element|value1|mflag1|qflag1|sflag1|value2|mflag2|qflag2|sflag2|value3|mflag3|qflag3|sflag3|value4|mflag4|qflag4|sflag4|value5|mflag5|qflag5|sflag5|value6|mflag6|qflag6|sflag6|value7|mflag7|qflag7|sflag7|value8|mflag8|qflag8|sflag8|value9|mflag9|qflag9|sflag9|value10|mflag10|qflag10|sflag10|value11|mflag11|qflag11|sflag11|value12|mflag12|qflag12|sflag12|value13|mflag13|qflag13|sflag13|value14|mflag14|qflag14|sflag14|value15|mflag15|qflag15|sflag15|value16|mflag16|qflag16|sflag16|value17|mflag17|qflag17|sflag17|value18|mflag18|qflag18|sflag18|value19|mflag19|qflag19|sflag19|value20|mflag20|qflag20|sflag20|value21|mflag21|qflag21|sflag21|value22|mflag22|qflag22|sflag22|value23|mflag23|qflag23|sflag23|value24|mflag24|qflag24|sflag24|value25|mflag25|qflag25|sflag25|value26|mflag26|qflag26|sflag26|value27|mflag27|qflag27|sflag27|value28|mflag28|qflag28|sflag28|value29|mflag29|qflag29|sflag29|value30|mflag30|qflag30|sflag30|value31|mflag31|qflag31|sflag31|
* Example
|CA004011580|1957|   02|   TMAX|  -156|      |      |     C|  -106|      |      |     C|  -106|      |      |     C|   -61|      |      |     C|  -117|      |      |     C|  -117|      |      |     C|   -78|      |      |     C|   -67|      |      |     C|    22|      |      |     C|     17|       |       |      C|     17|       |       |      C|     17|       |       |      C|     39|       |       |      C|     56|       |       |      C|    -22|       |       |      C|      6|       |       |      C|    -78|       |       |      C|   -150|       |       |      C|   -178|       |       |      C|   -183|       |       |      C|   -228|       |       |      C|   -206|       |       |      C|   -100|       |       |      C|     89|       |       |      C|    -56|       |       |      C|     67|       |       |      C|     44|       |       |      C|     -6|       |       |      C|  -9999|       |       |       |  -9999|       |       |       |  -9999|       |       |       |
EMR Cluster Setup

## Athena Queries
TODO