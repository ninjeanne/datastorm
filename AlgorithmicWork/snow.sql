----------------------------------Query to extract data for snow-depth for different months of the year----------------------------------

SELECT station, "station name", latitude, longitude, elevation, state, date_format(date_parse(date,'%Y%m%d'), '%m') AS "month", avg(value) AS "avg_value"
FROM observations
WHERE observation = 'SNWD' 
GROUP BY station, "station name", date_format(date_parse(date,'%Y%m%d'), '%m'), latitude, longitude, elevation, state
ORDER BY "avg_value" DESC




----------------------------------Query to extract data for snow-depth, maximum temperature, minimum temperature and average temperature----------------------------------

SELECT station, "station name", latitude, longitude, elevation, state, date_format(date_parse(date,'%Y%m%d'), '%Y%m') AS "year-month", observation, avg(value) AS "avg_value"
FROM datastorm.observations
WHERE observation in ('SNWD', 'TMAX', 'TMIN', 'TAVG') 
GROUP BY station, "station name", date_format(date_parse(date,'%Y%m%d'), '%Y%m'), latitude, longitude, elevation, state, observation

----------------------------------Query to extract data of Rain, Wind and Snow----------------------------------
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



