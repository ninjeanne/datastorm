### Project Ideas

hdfs dfs -ls /courses/datasets/ghcn
hdfs dfs -ls /courses/datasets/ghcn-splits
hdfs dfs -cat /courses/datasets/README-ghcn.md

predict catastrophes based on temperature and other weather data

sample:
```
tail 1763-a.csv
ITE00100554,17631230,TMAX,81,,,E,
ITE00100554,17631230,TMIN,61,,I,E,
ITE00100554,17631231,TMAX,79,,,E,
ITE00100554,17631231,TMIN,59,,,E,
```

```
hdfs dfs -copyToLocal /courses/datasets/ghcn/1763.csv.gz
gunzip 1763.csv.gz
tail 1763.csv
ITE00100554,17631230,TMAX,81,,,E,
ITE00100554,17631230,TMIN,61,,I,E,
ITE00100554,17631231,TMAX,79,,,E,
ITE00100554,17631231,TMIN,59,,,E,
```

# About the GHCN data set
SOURCE: https://www.ncei.noaa.gov/products/land-based-station/global-historical-climatology-network-daily \
DOCUMENTATION: https://www.ncei.noaa.gov/data/global-historical-climatology-network-daily/doc/GHCND_documentation.pdf \
BETTER DOC: https://www1.ncdc.noaa.gov/pub/data/ghcn/daily/readme.txt \
STATIONS: https://www1.ncdc.noaa.gov/pub/data/ghcn/daily/ghcnd-stations.txt

The `ghcn-more` directory contains the `ghcnd-stations.txt` list of weather stations and `ghcnd-inventory.txt` summary of available observations.

If the original data format is too clumpy, my recipe to make smaller input files for better partitioning is:

```
seq 1763 2016 | parallel -j 4 "zcat ghcn/{}.csv.gz | split -C200M -a 1 --additional-suffix='.csv' --filter='gzip > \$FILE.gz' - ghcn-splits/{}-"
```
