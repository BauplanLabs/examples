"""

    Collection of realistic Bauplan tasks (small, big, queries, runs, with / without writes), so that the main
    loop can randomly pick from the list and generate some stats after running the tasks with the predefined
    level of concurrency

"""

import bauplan
import threading
import pyarrow as pa


def small_query(profile: str, task_id: str) -> bool:
    print(f"[{threading.current_thread().name}] Executing small query - Task ID: {task_id}")
    bpl_client = bauplan.Client(profile=profile)
    rows = bpl_client.query("""
        SELECT 
            SUM(trip_time)
        FROM  taxi_fhvhv
        WHERE pickup_datetime >= '2022-12-30T00:00:00-05:00'
        AND pickup_datetime < '2023-01-01T00:00:00-05:00'
    """.strip(), cache='off')
    assert type(rows) is pa.Table, "Expected a PyArrow Table from the query"
    assert len(rows) == 1, "No rows returned from small query"
    del bpl_client
    
    return True


def big_query(profile: str, task_id: str) -> bool:
    print(f"[{threading.current_thread().name}] Executing big query - Task ID: {task_id}")
    bpl_client = bauplan.Client(profile=profile)
    rows = bpl_client.query("""
        SELECT 
            pickup_datetime,
            PULocationID,
            trip_miles,
            trip_time
        FROM  taxi_fhvhv
        WHERE pickup_datetime >= '2022-07-30T00:00:00-05:00'
        AND pickup_datetime < '2023-01-01T00:00:00-05:00'
    """.strip(), cache='off')
    assert type(rows) is pa.Table, "Expected a PyArrow Table from the query"
    assert len(rows) > 0, "No rows returned from the query"
    del bpl_client
    
    return True


def small_pipeline(profile: str, task_id: str, dry_run: bool = False) -> bool:
    print(f"[{threading.current_thread().name}] Executing small pipeline, dry_run {dry_run} - Task ID: {task_id}")
    bpl_client = bauplan.Client(profile=profile)
    user = bpl_client.info().user
    username = user.username
    tmp_branch_name = f'{username}.small_pipeline_{task_id}'
    bpl_client.create_branch(tmp_branch_name, 'main')
    run_state = bpl_client.run(
        'bpln_pipeline/',
        ref=tmp_branch_name,
        cache='off',
        dry_run=dry_run
    )
    bpl_client.delete_branch(tmp_branch_name)
    del bpl_client
    
    if run_state.job_status.lower() != 'success':
        raise Exception("Run not completed!")
    
    return True


def big_pipeline(profile: str, task_id: str, dry_run: bool = False) -> bool:
    print(f"[{threading.current_thread().name}] Executing big pipeline, dry_run {dry_run} - Task ID: {task_id}")
    bpl_client = bauplan.Client(profile=profile)
    user = bpl_client.info().user
    username = user.username
    tmp_branch_name = f'{username}.big_pipeline_{task_id}'
    bpl_client.create_branch(tmp_branch_name, 'main')
    run_state = bpl_client.run(
        'bpln_pipeline/',
        ref=tmp_branch_name,
        cache='off',
        dry_run=dry_run,
        parameters={'start_trip_date': '2022-05-01T00:00:00-05:00'}
    )
    bpl_client.delete_branch(tmp_branch_name)
    del bpl_client
    
    if run_state.job_status.lower() != 'success':
        raise Exception("Run not completed!")
    
    return True