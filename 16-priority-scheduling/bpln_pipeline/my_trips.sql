SELECT 
    pickup_datetime,
    PULocationID,
    trip_miles,
    trip_time
FROM  taxi_fhvhv
WHERE pickup_datetime >= $start_trip_date
AND pickup_datetime < '2023-01-01T00:00:00-05:00'