import bauplan


@bauplan.model(materialization_strategy='REPLACE')
@bauplan.python('3.10')
def trips_and_zones(
    trips=bauplan.Model('my_trips'),   
    zones=bauplan.Model('taxi_zones', columns=['LocationID', 'Borough'])
):
    pickup_location_table = trips.join(zones, 'PULocationID', 'LocationID').combine_chunks()
    return pickup_location_table
