----------------------------------Ouery to find extract data for snow-depth for different months of the year----------------------------------

SELECT station, "station name", latitude, longitude, elevation, state, date_format(date_parse(date,'%Y%m%d'), '%m') AS "month", avg(value) AS "avg_value"
FROM observations
WHERE observation = 'SNWD' 
GROUP BY station, "station name", date_format(date_parse(date,'%Y%m%d'), '%m'), latitude, longitude, elevation, state
ORDER BY "avg_value" DESC




----------------------------------Ouery to find extract data for snow-depth, maximum temperature, minimum temperature and average temperature----------------------------------

SELECT station, "station name", latitude, longitude, elevation, state, date_format(date_parse(date,'%Y%m%d'), '%Y%m') AS "year-month", observation, avg(value) AS "avg_value"
FROM datastorm.observations
WHERE observation in ('SNWD', 'TMAX', 'TMIN', 'TAVG') 
GROUP BY station, "station name", date_format(date_parse(date,'%Y%m%d'), '%Y%m'), latitude, longitude, elevation, state, observation
