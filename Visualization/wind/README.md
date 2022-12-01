# Results concerning the wind

From the GeoMap results [here](GeoMap_2022-11-26T02_48_09.pdf), you can see the location of Canadian stations that are recording WSFG (wind speed data). 

To find out from which time range we can get access to the most wind speed data, we ran the following query in Athena:
```sql
-- number of WSFG per year per state (sorted by state and count)
select state, count(observation) as count_wsfg, date_format(date_parse(date,'%Y%m%d'), '%Y') as year from datastorm.observations where observation = 'WSFG' group by state, observation, date_format(date_parse(date,'%Y%m%d'), '%Y') order by state, count_wsfg desc, year
-- the most values come from 2016 and 2017 (one exception: PE: 2016, 2020, 2017). The collection started in 2015. Exception: NL (1950-1970)
```
We noticed that between 1945 and 1970, wind speed values were recorded for the province of New Foundland and Labrador. The other provinces and territories didn't record anything for this observation.
But starting with 2015, the WSFG was recorded for Canada or at least a plethora of its stations. So we can work with windspeed-related data beginning in 2015.
The other observation types for wind that we wanted to use aren't provided for Canada. The only further wind-related observation is the weather type WT03 (thunder). 
As with the windspeed, thunder was only recorded between 1945 to 1970 and not for every province or territory. So we can't rely on WT03, too.

As a next step, we wanted to determine which state would be optimal for wind and temperature (and also to find any correlation between these observations).
We grouped the data after the state and months for each year starting from 2015. 

```sql
-- grouped by year and month and by state
SELECT
    t1.state,
    date_format(date_parse(t1.date,'%Y%m%d'), '%Y%m') as date,
    avg(cast(t1.value AS DOUBLE)) as wsfg,
    avg(cast(t2.value AS DOUBLE)/10) as tmax,
    avg(cast(t3.value AS DOUBLE)/10) as tmin,
    avg(cast(t4.value AS DOUBLE)/10) as tavg
FROM datastorm.observations t1
    INNER JOIN datastorm.observations t2 ON t1.station = t2.station AND t1.date = t2.date AND t2.observation = 'TMAX'
    INNER JOIN datastorm.observations t3 ON t1.station = t3.station AND t1.date = t3.date AND t1.observation = 'WSFG' AND t3.observation = 'TMIN'
    INNER JOIN datastorm.observations t4 ON t1.station = t4.station AND t1.date = t4.date AND t4.observation = 'TAVG'
GROUP BY t1.state, date_format(date_parse(t1.date,'%Y%m%d'), '%Y%m')
```
The results were visualized within a [timeseries plot](timeseries_state_2022-11-26T21_56_13.pdf), a [heatmap](heatmap_2022-11-26T22_21_32.pdf), a [Tree Map](heatmap_2022-11-26T22_21_32.pdf) and a [boxplot](BoxPlots_2022-11-26T02_45_38.pdf).

The time-series plot shows that every Canadian state's wind speed started to increase after 2018.
The state with the most wind in average, max, as well as the highest median, is NL (Newfoundland and Labrador), followed by British Columbia (BC), Nova Scotia (NS) and Prince Edward Island (PE).
But these states are also where the temperature has no high peaks or drops. Especially in BC, the temperature has low magnitudes.
The temperature in the territories (Nunavut (NT), Northern Territories (NT) and Yukon (YT)) are too cold to be considered a good choice for a settlement even though the wind is more stable there. Plus, there are few stations on which data we can rely.

In conclusion, we will continue our research on the southern states.

The final [Geomap](Heatmap_with_Geodata_2022-11-27T00_50_24.pdf) visualizes the max, min and average of the wind, max, min and average temperature.

According to this plot, the best place to live with respect to temperature and wind is the Greater Vancouver Area!

