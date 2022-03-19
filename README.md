# aws-homework

Architecture View：

![arch](https://user-images.githubusercontent.com/7961235/159115721-8fefdbb9-da27-4023-beee-ecbb3d14f57e.png)

File description：

process.sh 

main process script，copy travel data from public S3 source into my S3 bucket, then use EMR(hive) to execute ETL process

process.sql

ETL script, create four tables for yellow, green fhv, fhvhv trip data as original data table, and create a table 'dim_taxi_zone_lookup" for location query, then extract data from four trip data table and load into the target table 'dw_trip_data_all'

process_athena.sql

Athena data source definition file, define two table 'dm_trip_data_all' and 'dw_trip_data_all' for AWS Quickview. 
