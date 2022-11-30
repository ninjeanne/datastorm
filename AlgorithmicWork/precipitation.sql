-------------------- Query to create dataset with required columns of interest grouped by station & date (from 2015) to create Geomap --------------------
SELECT station, "station name", latitude, longitude, elevation, state, date_format(date_parse(date,'%Y%m%d'), '%Y%m') AS "year-month", observation, avg(value) AS "avg_value"
FROM datastorm.observations
WHERE observation in ('PRCP', 'TMAX', 'TMIN', 'TAVG', 'WT05', 'WT16') AND date_format(date_parse(date,'%Y%m%d'), '%Y%m') >= '20150101'
GROUP BY station, "station name", date_format(date_parse(date,'%Y%m%d'), '%Y%m'), latitude, longitude, elevation, state, observation


-------------------- Query to create dataset of required columns of interest grouped by YYYYMM (from 2015), station and state to create Line Charts --------------------
SELECT
    t1.state,
    date_format(date_parse(t1.date,'%Y%m%d'), '%Y%m') as date,
    avg(cast(t1.value AS DOUBLE)) as prcp,
    avg(cast(t2.value AS DOUBLE)/10) as tmax,
    avg(cast(t3.value AS DOUBLE)/10) as tmin,
    avg(cast(t4.value AS DOUBLE)/10) as tavg


FROM datastorm.observations t1
    INNER JOIN datastorm.observations t2 ON t1.station = t2.station AND t1.date = t2.date AND t2.observation = 'TMAX'
    INNER JOIN datastorm.observations t3 ON t1.station = t3.station AND t1.date = t3.date AND t3.observation = 'TMIN' AND t1.observation = 'PRCP'
    INNER JOIN datastorm.observations t4 ON t1.station = t4.station AND t1.date = t4.date AND t4.observation = 'TAVG'
WHERE date_format(date_parse(t1.date,'%Y%m%d'), '%Y%m') >= '20150101'
GROUP BY t1.state, date_format(date_parse(t1.date,'%Y%m%d'), '%Y%m')