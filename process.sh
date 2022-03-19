--- init code
// get data from 2015 to 2021
for i in 2015 2016 2017 2018 2019 2020 2021
do
aws s3 cp s3://nyc-tlc/trip\ data/ s3://bigdata-tang/ods_yellow_tripdata_all/ --recursive --exclude "*" --include "yellow_tripdata_$i*"
aws s3 cp s3://nyc-tlc/trip\ data/ s3://bigdata-tang/ods_green_tripdata_all/ --recursive --exclude "*" --include "green_tripdata_$i*"
done
aws s3 cp s3://nyc-tlc/trip\ data/ s3://bigdata-tang/ods_fhv_tripdata_all/ --recursive --exclude "*" --include "fhv_*"
aws s3 cp s3://nyc-tlc/trip\ data/ s3://bigdata-tang/ods_fhvhv_tripdata_all/ --recursive --exclude "*" --include "fhvhv*"

aws s3 cp s3://bigdata-tang/process.sql .
hive -f process.sql
