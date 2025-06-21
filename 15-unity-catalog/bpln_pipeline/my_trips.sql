SELECT 
    pickup_datetime,
    PULocationID,
    trip_miles,
    trip_time
FROM  taxi_fhvhv
WHERE pickup_datetime >= '2022-12-15T00:00:00-05:00' 
AND pickup_datetime < '2023-01-01T00:00:00-05:00'