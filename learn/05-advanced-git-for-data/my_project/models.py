import bauplan


@bauplan.model(materialization_strategy="REPLACE")
def my_taxxxi_zones(
    data=bauplan.Model("bauplan.taxi_zones"),
    run_id=bauplan.Parameter("run_id"),
):
    """
    This model materializes a trivial table, which will allow us to vary the run_id from the caller
    to verify different rows get persisted.
    """
    import time

    return [
        {
            "run_id": run_id,
            "time_ns": time.time_ns(),
        }
    ]


@bauplan.model()
def my_taxxxi_zones_child(data=bauplan.Model("my_taxxxi_zones")):
    """
    This model is a child of the previous one, and it will be used to just test a scenario
    in which a pipeline fails with some artifacts written before the failure. This allows
    the caller to programmatically identify the temporary branch in which the failure occurred
    (sandboxed) and debug properly!
    """
    import time

    # Get the run_id from the parent model.
    run_id = data.to_pylist()[0]["run_id"]

    # This is useful to inject an error from the client!
    if run_id > 4:
        raise ValueError("run_id should be less than 5")

    return [
        {
            "run_id": run_id,
            "time_ns": time.time_ns(),
        }
    ]
