SELECT 
    pickup_datetime,
    PULocationID,
    trip_miles
FROM  
    taxi_fhvhv
WHERE pickup_datetime >= '2022-10-01T00:00:00-05:00' 
AND pickup_datetime < '2022-12-31T00:00:00-05:00'