# Analysis of snow-depth, TMIN, TAVG and TMAX
[QuickSight dashboard link](https://us-west-2.quicksight.aws.amazon.com/sn/analyses/a0715ac9-d30c-4b94-a057-90bf097a6b28)

## Query
```
SELECT station, "station name", latitude, longitude, elevation, state, date_format(date_parse(date,'%Y%m%d'), '%Y%m') AS "year-month", observation, avg(value) AS "avg_value"
FROM datastorm.observations
WHERE observation in ('SNWD', 'TMAX', 'TMIN', 'TAVG') 
GROUP BY station, "station name", date_format(date_parse(date,'%Y%m%d'), '%Y%m'), latitude, longitude, elevation, state, observation

```
[dataset link](https://us-west-2.quicksight.aws.amazon.com/sn/data-sets/62208cb7-e4f3-4b69-bcf5-18afa0a2493f/view)

## Filters
date-month - Jan 1 2015 to Feb 1 2015
States - AB, BC, MB, NB, NL, NS, ON, PE, QC, SK
snow depth avg_value - less than 400

## Plots
Geospatial plot 1 (Average-Average) - average of the snow depth over geospatial data point
Geospatial plot 2 (Average-Min)- Minimum of the snow depth over geospatial data point
Geospatial plot 3 (Average-Max)- Maxmum of the snow depth over geospatial data point

## Conclusion
With the help of these three plots we were able to visualize which regions in canada for the given provinces have the most optimal weather conditions- low average snow-depth and not too low average temperatures. Some of such regions in British Columbia were- Vancouver area and north west of Vancouver about 49.38 degree N and -123 degree W. We ignored the points that had no snow depth data to prevent false positives. 

# Analysis of snow depth from 2015 to 2021
[QuickSight dashboard link](https://us-west-2.quicksight.aws.amazon.com/sn/analyses/48432726-e0bf-4a2d-95b5-a5e0801ebbe6)

## Query
```
SELECT o1.station as station,
    o1.state as state,
    date_format(date_parse(o1.date,'%Y%m%d'), '%Y%m') as date,
    o1.elevation as elevation,
    o1.value as snow_depth,
    o2.value as wind_speed,
    o3.value as precipitation,
    o4.value/10 as tavg
FROM datastorm.observations o1
    INNER JOIN datastorm.observations o2 ON o1.station = o2.station AND o1.date = o2.date AND o1.observation = 'SNWD' AND o2.observation = 'WSFG' 
    INNER JOIN datastorm.observations o3 ON o1.station = o3.station AND o1.date = o3.date AND o3.observation = 'PRCP'
    INNER JOIN datastorm.observations o4 ON o1.station = o4.station AND o1.date = o4.date AND o4.observation = 'TAVG'
WHERE    
    date_parse(o1.date,'%Y%m%d') > CAST('2015-01-01' AS DATE) AND o1.value > 0 AND o3.value > 0 AND o3.value > 0
GROUP BY 
    date_format(date_parse(o1.date,'%Y%m%d'), '%Y%m'), o1.station, o1.state, o1.elevation, o1.value, o2.value, o3.value, o4.value

```
[dataset link](https://us-west-2.quicksight.aws.amazon.com/sn/data-sets/a1f7b30d-6a61-4a7a-9b5b-5c6ef46bc019/view)
## Filter
States - AB, BC, NS, ON, QC, SK

## Plots
line chart and area line chart

# Snow-Wind tradeoff and Snow-Precipitation tradeoff for years 2015-2021
dashboard link and query same as previous analysis

## Filter
state - BC

## Plots
done with stacked bar combo chart
snow-wind - snow depth and wind speeds are plotted for years 2015-2021 against average temperature
snow-precipitation - snow depth and precipitation are plotted for years 2015-2021 against average temperature

## Conclusion
Turns out wind speeds in BC have been high usually during the months of March and April, with it peaking in 2019 and snow depth has been lower in months of September and October over these couple of years.
