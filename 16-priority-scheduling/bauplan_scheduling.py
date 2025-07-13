import bauplan
from datetime import datetime
import random
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from typing import List, Optional, Tuple
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
from matplotlib.patches import Rectangle
import pyarrow as pa


@dataclass
class TaskResult:
    """Stores the result of a task execution"""
    task_id: str
    task_type: str
    success: bool
    error: Optional[Exception] = None
    start_time: Optional[datetime] = None
    end_time: Optional[datetime] = None
    duration: Optional[float] = None
    
    def __post_init__(self):
        if self.start_time and self.end_time:
            self.duration = (self.end_time - self.start_time).total_seconds()


# Bauplan SDK task functions

def small_query(profile: str, task_id: str) -> bool:
    print(f"[{threading.current_thread().name}] Executing small query - Task ID: {task_id}")
    bpl_client = bauplan.Client(profile=profile)
    rows = bpl_client.query("""
        SELECT 
            SUM(trip_time)
        FROM  taxi_fhvhv
        WHERE pickup_datetime >= '2022-12-30T00:00:00-05:00'
        AND pickup_datetime < '2023-01-01T00:00:00-05:00'
    """)
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
    """)
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
        'bpln_pipeline',
        ref=tmp_branch_name,
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
        'bpln_pipeline',
        ref=tmp_branch_name,
        dry_run=dry_run,
        parameters={'start_trip_date': '2022-05-01T00:00:00-05:00'}
    )
    bpl_client.delete_branch(tmp_branch_name)
    del bpl_client
    
    if run_state.job_status.lower() != 'success':
        raise Exception("Run not completed!")
    
    return True


# Task type mapping
TASK_TYPES = {
    "small_query": small_query,
    "big_query": big_query,
    "small_pipeline_dry_run": lambda profile, task_id: small_pipeline(profile, task_id, dry_run=True),
    "small_pipeline_materialization": lambda profile, task_id: small_pipeline(profile, task_id, dry_run=False),
    "big_pipeline_materialization": lambda profile, task_id: big_pipeline(profile, task_id, dry_run=False)
}


def execute_task(profile: str, task_type: str, task_id: str) -> TaskResult:
    result = TaskResult(
        task_id=task_id,
        task_type=task_type,
        success=False,
        start_time=datetime.now()
    )
    try:
        task_func = TASK_TYPES[task_type]
        success = task_func(profile, task_id)
        result.success = success
    except Exception as e:
        print(f"[{threading.current_thread().name}] ERROR: Task {task_id} failed: {str(e)}")
        result.error = e
    finally:
        result.end_time = datetime.now()
    
    return result


def generate_random_tasks(num_tasks: int, seed: int = 42) -> List[Tuple[str, str]]:
    random.seed(seed)
    task_types = list(TASK_TYPES.keys())
    tasks = []
    
    # First, ensure each task type appears at least once if we have enough tasks
    if num_tasks >= len(task_types):
        for i, task_type in enumerate(task_types):
            task_id = f"{task_type}_{i:03d}"
            tasks.append((task_type, task_id))
        
        # Fill the rest randomly
        for i in range(len(task_types), num_tasks):
            task_type = random.choice(task_types)
            task_id = f"{task_type}_{i:03d}"
            tasks.append((task_type, task_id))
    else:
        # If we have fewer tasks than task types, randomly select without replacement
        selected_types = random.sample(task_types, num_tasks)
        for i, task_type in enumerate(selected_types):
            task_id = f"{task_type}_{i:03d}"
            tasks.append((task_type, task_id))
    
    # Shuffle to randomize order
    random.shuffle(tasks)
    return tasks


def create_gantt_chart(results: List[TaskResult], filename: str):
    fig, ax = plt.subplots(figsize=(12, max(6, len(results) * 0.3)))
    
    # Sort results by start time
    sorted_results = sorted(results, key=lambda x: x.start_time)
    
    # Color mapping for task types
    colors = {
        "small_query": "#3498db",
        "big_query": "#e74c3c",
        "small_pipeline_dry_run": "#2ecc71",
        "small_pipeline_materialization": "#27ae60",
        "big_pipeline_dry_run": "#f39c12",
        "big_pipeline_materialization": "#e67e22"
    }
    
    # Create bars for each task
    for i, result in enumerate(sorted_results):
        start = mdates.date2num(result.start_time)
        end = mdates.date2num(result.end_time)
        duration = end - start
        
        color = colors.get(result.task_type, "#95a5a6")
        if not result.success:
            color = "#c0392b"  # Red for failed tasks
        
        ax.barh(i, duration, left=start, height=0.8, 
                color=color, alpha=0.8, 
                label=result.task_type if i == 0 else "")
        
        # Add task ID as text
        ax.text(start + duration/2, i, result.task_id, 
                ha='center', va='center', fontsize=8, color='white')
    
    # Format the plot
    ax.set_ylim(-0.5, len(results) - 0.5)
    ax.set_xlabel('Time')
    ax.set_ylabel('Tasks')
    ax.set_title('Task Execution Gantt Chart')
    
    # Format x-axis to show time
    ax.xaxis.set_major_formatter(mdates.DateFormatter('%H:%M:%S'))
    ax.xaxis.set_major_locator(mdates.SecondLocator(interval=1))
    plt.setp(ax.xaxis.get_majorticklabels(), rotation=45, ha='right')
    
    # Add legend
    handles = [Rectangle((0, 0), 1, 1, color=color) for task_type, color in colors.items()]
    handles.append(Rectangle((0, 0), 1, 1, color="#c0392b"))
    labels = list(colors.keys()) + ["Failed"]
    ax.legend(handles, labels, loc='upper right', bbox_to_anchor=(1.15, 1))
    
    # Add grid
    ax.grid(True, axis='x', alpha=0.3)
    
    plt.tight_layout()
    plt.savefig(filename, dpi=300, bbox_inches='tight')
    print(f"\nGantt chart saved to {filename}")
    plt.close()


def simulate_load(
    bpln_profile: str,
    seed: int,
    num_threads: int,
    num_tasks: int,
    chart_file_path: str
):
    ### Start up, and generate a random list of tasks to run
    
    print(f"Starting at {datetime.now()}\n with:")
    print(f"  Profile: {bpln_profile}")
    print(f"  Number of tasks: {num_tasks}")
    print(f"  Thread pool size: {num_threads}")
    print(f"  Random seed: {seed}")
    
    tasks = generate_random_tasks(num_tasks, seed)
    
    ### Run tasks with a thread pool
    
    results = []
    with ThreadPoolExecutor(max_workers=num_threads) as executor:
        future_to_task = {
            executor.submit(execute_task, bpln_profile, task_type, task_id): (task_type, task_id)
            for task_type, task_id in tasks
        }
        for future in as_completed(future_to_task):
            task_type, task_id = future_to_task[future]
            result = future.result()
            status = "SUCCESS" if result.success else "FAILED"
            print(f"[{threading.current_thread().name}] Task {task_id} completed - Status: {status}")
            results.append(result)
        
     
    assert results, "No tasks were executed. Check the task generation logic."   
        
    ### Summary stats and final chart    
        
    successful_tasks = sum(1 for r in results if r.success)
    failed_tasks = len(results) - successful_tasks
    
    print("\n" + "="*50)
    print("EXECUTION SUMMARY")
    print("="*50)
    print(f"Total tasks: {len(results)}")
    print(f"Successful: {successful_tasks}")
    print(f"Failed: {failed_tasks}")
    
    task_type_counts = {}
    for result in results:
        task_type_counts[result.task_type] = task_type_counts.get(result.task_type, 0) + 1
    
    print("\nTask type distribution:")
    for task_type, count in task_type_counts.items():
        print(f"  {task_type}: {count}")

    create_gantt_chart(results, chart_file_path)
    
    print(f"\n\nDone at {datetime.now()}\n\nSee you, space cowboy!")
    return
    
    
if __name__ == "__main__":
    # add some arguments
    import argparse
    parser = argparse.ArgumentParser(description="Simulate load for scheduling test.")
    parser.add_argument('--bpln_profile', default='default')
    parser.add_argument('--seed', type=int, default=42, help='Random seed for reproducibility')
    parser.add_argument('--num_threads', type=int, default=4, help='Number of threads to use for simulation')
    parser.add_argument('--num_tasks', type=int, default=10, help='Number of tasks to simulate')
    parser.add_argument('--chart_file_path', type=str, default='task_gantt_chart.png', help='Output path for Gantt chart')
    args = parser.parse_args()
    # now start the main function
    simulate_load(
        bpln_profile=args.bpln_profile,
        seed=args.seed,
        num_threads=args.num_threads,
        num_tasks=args.num_tasks,
        chart_file_path=args.chart_file_path
    )