# DataStorm

## Project Proposal
Our project proposal can be found [here](proposal/proposal.md).

## Init the project
### Get the data
```bash
# Create a data directory
mkdir data

# Get the metadata
## Stations
wget --directory-prefix=data/ https://www.ncei.noaa.gov/pub/data/ghcn/daily/ghcnd-stations.txt
## Inventory
wget --directory-prefix=data/ https://www.ncei.noaa.gov/pub/data/ghcn/daily/ghcnd-inventory.txt
## Country Codes
wget --directory-prefix=data/ https://www.ncei.noaa.gov/pub/data/ghcn/daily/ghcnd-countries.txt

# Get the data (7GB - might take some time!)
wget --directory-prefix=data/ https://www.ncei.noaa.gov/data/global-historical-climatology-network-daily/archive/daily-summaries-latest.tar.gz
## unpack
tar -xf data/daily-summaries-latest.tar.gz -C data/daily-summaries-latest
## keep only the Canadian data
mkdir data/canada
mv data/daily-summaries-latest/CA* data/canada
rm -rf data/daily-summaries-latest
```

## Run the project locally
### TODO

## Access the public dashboard
### TODO