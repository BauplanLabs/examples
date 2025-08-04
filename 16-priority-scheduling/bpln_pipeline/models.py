import bauplan

@bauplan.model()
@bauplan.python('3.10')
def trips_and_zones(
    trips=bauplan.Model('my_trips'),   
    zones=bauplan.Model('taxi_zones', columns=['LocationID', 'Borough'])
):
    pickup_location_table = trips.join(zones, 'PULocationID', 'LocationID').combine_chunks()
    return pickup_location_table


@bauplan.model(
    materialization_strategy='REPLACE',
    name='demo_trips',
)
@bauplan.python('3.11', pip={'pandas': '1.5.3', 'numpy': '1.23.2'})
def normalized_taxi_trips(
    data=bauplan.Model('trips_and_zones')
):
    import pandas as pd
    import numpy as np
    import math

    print(f"\n{round(data.nbytes / math.pow(1024, 3), 3)} GB, {data.num_rows} rows\n")
    df = data.to_pandas()
    df = df[df['trip_miles'] > 0.0]
    df = df[df['trip_miles'] < 200.0]
    df['log_trip_miles'] = np.log10(df['trip_miles'])

    return df
