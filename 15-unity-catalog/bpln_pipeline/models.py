import bauplan


@bauplan.model(
    columns=[ 
        'pickup_datetime',
        'PULocationID',
        'trip_miles',
        'trip_time',
        'Borough'
    ]             
)
@bauplan.python('3.10')
def trips_and_zones(
        trips=bauplan.Model('my_trips'),   
        zones=bauplan.Model('taxi_zones', columns=['LocationID', 'Borough'])
):
    pickup_location_table = trips.join(zones, 'PULocationID', 'LocationID').combine_chunks()
    return pickup_location_table


@bauplan.model(materialization_strategy='REPLACE')
@bauplan.python('3.11', pip={'pandas': '2.2', 'numpy': '1.23.2'})
def unity_trips(
    data=bauplan.Model('trips_and_zones')
):
    import pandas as pd
    import numpy as np
    import math

    size_in_gb = round(data.nbytes / math.pow(1024, 3), 3)
    print(f"\nThis table is {size_in_gb} GB and has {data.num_rows} rows\n")
    df = data.to_pandas()
    time_filter = pd.to_datetime('2022-01-01')
    time_filter_utc = time_filter.tz_localize('UTC')
    df = df[df['pickup_datetime'] >= time_filter_utc]
    df = df[df['trip_miles'] > 0.0]
    df = df[df['trip_miles'] < 200.0]
    df['log_trip_miles'] = np.log10(df['trip_miles'])

    return df

