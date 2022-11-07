# DataStorm

## Project Proposal
Our project proposal can be found [here](proposal/proposal.md).

## Init the project
### Get the data
Link to the [official repository](https://www.ncei.noaa.gov/pub/data/ghcn/daily/) and the [documentation](https://www1.ncdc.noaa.gov/pub/data/ghcn/daily/readme.txt)
```bash
# Create a data directory
DATA_DIR=data
mkdir $DATA_DIR

# Get the metadata
## Stations
wget --directory-prefix=$DATA_DIR/ https://www.ncei.noaa.gov/pub/data/ghcn/daily/ghcnd-stations.txt
## States
wget --directory-prefix=$DATA_DIR/ https://www.ncei.noaa.gov/pub/data/ghcn/daily/ghcnd-states.txt
## Inventory
wget --directory-prefix=$DATA_DIR/ https://www.ncei.noaa.gov/pub/data/ghcn/daily/ghcnd-inventory.txt
## Country Codes
wget --directory-prefix=$DATA_DIR/ https://www.ncei.noaa.gov/pub/data/ghcn/daily/ghcnd-countries.txt

# Get daily-summaries-latest (7GB - might take some time!)
wget --directory-prefix=$DATA_DIR/ https://www.ncei.noaa.gov/data/global-historical-climatology-network-daily/archive/daily-summaries-latest.tar.gz
echo "unpack daily-summaries-latest.tar.gz"
tar -xf $DATA_DIR/daily-summaries-latest.tar.gz -C $DATA_DIR/
mkdir $DATA_DIR/ghcnd-daily-summaries-latest-canada
echo "keep only the Canadian data"
mv $DATA_DIR/daily-summaries-latest/CA* $DATA_DIR/ghcnd-daily-summaries-latest-canada
echo "clean up"
rm -rf $DATA_DIR/daily-summaries-latest
rm $DATA_DIR/daily-summaries-latest.tar.gz

# Get ghcnd_all (3GB - might take some time!)
wget --directory-prefix=$DATA_DIR/ https://www.ncei.noaa.gov/pub/data/ghcn/daily/ghcnd_all.tar.gz
echo "unpack ghcnd_all.tar.gz"
tar -xf $DATA_DIR/ghcnd_all.tar.gz -C $DATA_DIR
mkdir $DATA_DIR/ghcnd-all-canada
echo "keep only the Canadian data"
mv $DATA_DIR/ghcnd_all/CA* data.nosync/ghcnd-all-canada
echo "clean up"
rm -rf $DATA_DIR/ghcnd_all
rm $DATA_DIR/ghcnd_all.tar.gz
```

## Run the project locally
### TODO

## Access the public dashboard
### TODO