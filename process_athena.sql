-- get data into day --Athena
create table dm_trip_data_all
with(external_location = 's3://bigdata-tang/dm_trip_data_all', format ='PARQUET')
as
select a.dt, a.cnt, a.trip_time, a.trip_distance,  a.trip_distance/a.trip_time*60*60 as trip_speed from (
select 
date(pickup_datetime) as dt,
count(1) as cnt,
sum(if(to_unixtime(dropoff_datetime)-to_unixtime(pickup_datetime)> 0, to_unixtime(dropoff_datetime)-to_unixtime(pickup_datetime), 0)) as trip_time,
sum(if(trip_distance > 0, trip_distance, 0)) as trip_distance
from dw_trip_data_all
where date(pickup_datetime) > date('2015-01-01')
group by 
date(pickup_datetime)
) a
-- mingxi 
create table dw_trip_data_all (
    pickup_datetime timestamp,
    dropoff_datetime timestamp,
    trip_time double,
    trip_distance double,
    trip_speed double,
    PULocationID int,
    PUborough string,
    PUzone string,
    PUservice_zone string,
    DOLocationID int,
    DOborough string,
    DOzone string,
    DOservice_zone string
)
stored as parquet
location "s3://bigdata-tang/dw_trip_data_all/" 
