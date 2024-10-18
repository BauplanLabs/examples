import bauplan

@bauplan.model()
@bauplan.python('3.11')
def model(
        data=bauplan.Model(
            'taxi_fhvhv',
            columns=[
                'pickup_datetime',
                'dropoff_datetime',
                'PULocationID',
                'DOLocationID',
                'trip_miles',
                'trip_time',
                'base_passenger_fare',
                'tolls',
                'sales_tax',
                'tips',
            ],
            filter="pickup_datetime >= '2022-12-01T00:00:00-05:00' AND pickup_datetime < '2023-01-01T00:00:00-05:00'"
                    ),
            ):

    return data