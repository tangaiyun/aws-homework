drop table if exists ods_yellow_tripdata_all;
create external table ods_yellow_tripdata_all (
    VendorID int,
    tpep_pickup_datetime timestamp,
    tpep_dropoff_datetime timestamp,
    passenger_count int,
    trip_distance double,
    RatecodeID int,
    store_and_fwd_flag string,
    PULocationID int,
    DOLocationID int,
    payment_type int,
    fare_amount double,
    extra int,
    mta_tax double,
    tip_amount double,
    tolls_amount double,
    improvement_surcharge double,
    total_amount double,
    congestion_surcharge double 
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
location "s3://bigdata-tang/ods_yellow_tripdata_all/"
tblproperties ("skip.header.line.count"="1");

drop table if exists ods_green_tripdata_all;
create external table ods_green_tripdata_all (
    VendorID int,
    tpep_pickup_datetime timestamp,
    tpep_dropoff_datetime timestamp,
    passenger_count int,
    trip_distance double,
    RatecodeID int,
    store_and_fwd_flag string,
    PULocationID int,
    DOLocationID int,
    payment_type int,
    fare_amount double,
    extra int,
    mta_tax double,
    tip_amount double,
    tolls_amount double,
    improvement_surcharge double,
    total_amount double,
    trip_type int,
    congestion_surcharge double 
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
location "s3://bigdata-tang/ods_green_tripdata_all/"
tblproperties ("skip.header.line.count"="1"); 

drop table if exists ods_fhv_tripdata_all;
create external table ods_fhv_tripdata_all (
    dispatching_base_num string,
    pickup_datetime timestamp,
    dropoff_datetime timestamp,
    PULocationID int,
    DOLocationID int,
    SR_Flag string,
    Affiliated_base_number string
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
location "s3://bigdata-tang/ods_fhv_tripdata_all/"
tblproperties ("skip.header.line.count"="1"); 

drop table if exists ods_fhvhv_tripdata_all;
create external table ods_fhvhv_tripdata_all (
    hvfhs_license_num string,
    dispatching_base_num string,
    pickup_datetime timestamp,
    dropoff_datetime timestamp,
    PULocationID int,
    DOLocationID int,
    SR_Flag string
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
location "s3://bigdata-tang/ods_fhvhv_tripdata_all/"
tblproperties ("skip.header.line.count"="1"); 

drop table if exists dim_taxi_zone_lookup;
create external table dim_taxi_zone_lookup (
    locationID int,
    borough string,
    zone string,
    service_zone string
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
location "s3://bigdata-tang/dim_taxi_zone_lookup/"
tblproperties ("skip.header.line.count"="1"); 

-- result
drop table if exists dw_trip_data_all;
create external table dw_trip_data_all (
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


-- sql build result
drop table if exists tmp_trips_data_all;
create table tmp_trips_data_all stored as parquet
as
select 
a.pickup_datetime,
a.dropoff_datetime,
-- second(a.dropoff_datetime-a.pickup_datetime) as trip_time,
(unix_timestamp(dropoff_datetime)-unix_timestamp(pickup_datetime)) as trip_time,
a.trip_distance,
a.trip_distance/(unix_timestamp(dropoff_datetime)-unix_timestamp(pickup_datetime)) as trip_speed,
a.PULocationID,
b.borough as PUborough,
b.zone as PUzone,
b.service_zone as PUservice_zone,
a.DOLocationID,
c.borough as DOborough,
c.zone as DOzone,
c.service_zone as DOservice_zone
from (
select 
    tpep_pickup_datetime as pickup_datetime,
    tpep_dropoff_datetime as dropoff_datetime,
    trip_distance,
    PULocationID,
    DOLocationID 
from ods_yellow_tripdata_all
union all
select 
    tpep_pickup_datetime as pickup_datetime,
    tpep_dropoff_datetime as dropoff_datetime,
    trip_distance,
    PULocationID,
    DOLocationID
from ods_green_tripdata_all
union all
select 
    pickup_datetime,
    dropoff_datetime,
    0 as trip_distance,
    PULocationID,
    DOLocationID
from ods_fhv_tripdata_all
union all
select  
    pickup_datetime,
    dropoff_datetime,
    0 as trip_distance,
    PULocationID,
    DOLocationID
from ods_fhvhv_tripdata_all
) a
left join dim_taxi_zone_lookup b on a.PULocationID = b.locationID
left join dim_taxi_zone_lookup c on a.DOLocationID = c.locationID;

-- group by day and get the result
-- insert
insert overwrite table dw_trip_data_all
select * from tmp_trips_data_all;
