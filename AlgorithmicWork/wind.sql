select state, station, count(observation) as number_of_observations, observation from datastorm.observations where observation in ('TMAX','TMIN', 'WT03', 'WSFG', 'WT11', 'WT12', 'WT10', 'DATX', 'MDTX', 'DATN', 'MDTN', 'WDFI', 'WDFG', 'WSFG') group by station, observation, state order by number_of_observations desc;

-- TMAX
-- count of tmax
select state, station, count(observation) as number_of_observations, observation from datastorm.observations where observation = 'TMAX' group by station, observation, state order by number_of_observations desc;
-- stats for tmax
select state, max(value)/10 as max_tmax, avg(value)/10 as avg_tmax, min(value)/10 as min_tmax from datastorm.observations where observation = 'TMAX' group by state, observation order by avg_tmax desc;

-- TMIN
-- stats for tmin
select state, max(value)/10 as max_tmin, avg(value)/10 as avg_tmin, min(value)/10 as min_tmin from datastorm.observations where observation = 'TMIN' group by state, observation order by avg_tmin desc;

-- TAVG
-- stats for tavg
select state, max(value)/10 as max_tavg, avg(value)/10 as avg_tavg, min(value)/10 as min_tavg from datastorm.observations where observation = 'TAVG' group by state, observation order by avg_tavg desc;


-- WT03
-- stats for wt03
select state, max(value) as max_wt03, avg(value) as avg_wt03, min(value) as min_wt03 from datastorm.observations where observation = 'WT03' group by state, observation order by avg_wt03 desc;
-- doesn't make any sense. returns 1 for min and max. only 3 states

-- number of WT03 per year per state (sorted by state and count)
select state, count(observation) as count_wt03, date_format(date_parse(date,'%Y%m%d'), '%Y') as year from datastorm.observations where observation = 'WT03' group by state, observation, date_format(date_parse(date,'%Y%m%d'), '%Y') order by year, state, count_wt03 desc
-- problem: 1945-1970 collected for NL, 1949 for NU (1) and 1949 for QC (11)

-- WSFG
-- stats for WSFG
select state, max(value) as max_wsfg, avg(value) as avg_wsfg, min(value) as min_wsfg from datastorm.observations where observation = 'WSFG' group by state, observation order by avg_wsfg desc
-- meters per second, always positive (0 is the min)

-- stats for WSFG after 2000
select state, max(value) as max_wsfg, avg(value) as avg_wsfg, min(value) as min_wsfg, min(date) as min_date from datastorm.observations where observation = 'WSFG' and date_parse(date,'%Y%m%d') > CAST('2000-01-01' AS DATE) group by state, observation order by avg_wsfg desc
-- returns for the min date only values for 2015?? wtf

-- number of WSFG per year per state (sorted by state and count)
select state, count(observation) as count_wsfg, date_format(date_parse(date,'%Y%m%d'), '%Y') as year from datastorm.observations where observation = 'WSFG' group by state, observation, date_format(date_parse(date,'%Y%m%d'), '%Y') order by state, count_wsfg desc, year
-- the most values come from 2016 and 2017 (one exception: PE: 2016, 2020, 2017). Collection seems to have started in 2015. Exception: NL (1950-1970)






