# UI and Visualization

As described in [Technologies](../Technologies), we decided to use Amazon QuickSight as our visualization tool.

# Final results
1. [Snow](Snow)
2. [Wind](wind)
3. [Rain](Rain)

!!!! TODO talk about the result

# Optimization
From our initial question to analyzing the data, until we were finally able to visualize our result, we had to make many changes in our architecture and approaches.
Even though we transformed the data such that we can query over different measurements per day more efficiently and even though we joined
the metadata of stations with the measurements, the grade of denormalization still needs to be increased.
We experienced long loading times when querying the data with Quicksight and Athena.
That is because we aggregated the values per station, state and date and calculated the count, min, 
max, sum or average, grouped by dates repeatedly for different observations.
Instead of using cases and inner joins on our data table, we would initially add another transformation to our pipeline that does this for us.
In this case, we would save more computation power and could focus on more complex queries on our dataset, with which we can come to even more detailed insights.
Also, as our dataset is small compared to other data, we were thinking of transforming it into more than just one table, depending on our queries.

A further transformed table that has already been aggregated over different observations per station per month could look like the following:
State | Station | Longitude | Latitude | Elevation | YYYYMM | TMAX | TMIN | TAVG | SNOW | PRCP | WSFG | ...

A table that has already been aggregated over different observations per state could look like this:
State | TMAX | TMIN | TAVG | SNOW | PRCP | WSFG | ...

A table that groups values for a range of longitudes (or latitudes and elevations) could look like this:
Longitude Range | TMAX | TMIN | TAVG | SNOW | PRCP | WSFG | ...

We found that XX, YY and ZZ (TODO) might be considered the best places for a refugee settlement. 
But if this research should continue to gain deeper insights and with a team of skilled meteorologists, we are convinced that our architecture will fit their needs.
