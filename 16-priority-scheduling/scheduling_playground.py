from datetime import datetime
import random
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from typing import List, Optional, Tuple
import matplotlib.pyplot as plt
from matplotlib.patches import Rectangle
from tasks import small_pipeline, big_pipeline, small_query, big_query


# Task type mapping
TASK_TYPES = {
    "small_query": small_query,
    "big_query": big_query,
    "small_pipeline_dry_run": lambda profile, task_id: small_pipeline(profile, task_id, dry_run=True),
    "small_pipeline_materialization": lambda profile, task_id: small_pipeline(profile, task_id, dry_run=False),
    "big_pipeline_materialization": lambda profile, task_id: big_pipeline(profile, task_id, dry_run=False)
}

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
        result.error = e
    finally:
        result.end_time = datetime.now()
        result.duration = (result.end_time - result.start_time).total_seconds()
    
    return result


def generate_random_tasks(num_tasks: int, seed: int = 42) -> List[Tuple[str, str]]:
    random.seed(seed)
    task_types = list(TASK_TYPES.keys())
    tasks = []
    
    # Ensure each task type appears at least once if we have enough tasks
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
    fig, ax = plt.subplots(figsize=(8, 4))
    
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
    
    # Find the earliest start time to use as reference
    min_start_time = min(result.start_time for result in sorted_results)
    
    # Create bars for each task
    for i, result in enumerate(sorted_results):
        # Calculate start and duration in seconds from the reference time
        start_seconds = (result.start_time - min_start_time).total_seconds()
        duration_seconds = result.duration
        
        color = colors.get(result.task_type, "#95a5a6")
        if not result.success:
            color = "#c0392b"  # Red for failed tasks
        
        ax.barh(i, duration_seconds, left=start_seconds, height=0.8, 
                color=color, alpha=0.8, 
                label=result.task_type if i == 0 else "")
        
        # Add a shorter version of task ID as text
        short_task_id = f"{result.task_id[:5]}_{result.task_id.split('_')[-1]}"
        ax.text(start_seconds, i, short_task_id, ha='left', va='center', fontsize=8, color='black')

    # Format the plot
    ax.set_ylim(-0.5, len(results))
    ax.set_xlabel('Time (seconds)')
    ax.set_ylabel('')
    ax.set_yticks([])
    
    # Set x-axis limits
    max_end_seconds = max((result.start_time - min_start_time).total_seconds() + result.duration 
                          for result in sorted_results)
    ax.set_xlim(0, max_end_seconds * 1.05)  # Add 5% padding
    
    # Add legend
    handles = [Rectangle((0, 0), 1, 1, color=color) for task_type, color in colors.items()]
    handles.append(Rectangle((0, 0), 1, 1, color="#c0392b"))
    labels = list(colors.keys()) + ["Failed"]
    ax.legend(handles, labels, loc='upper right', bbox_to_anchor=(1.15, 1))
    
    # Add grid
    ax.grid(True, axis='x', alpha=0.3)
    
    plt.savefig(filename, bbox_inches='tight')
    print(f"\nChart saved to {filename}")
    plt.close()
    
    return


def draw_query_vs_pipeline_duration(results: List[TaskResult], filename: str, size: str = "small"):
    # Filter durations based on size parameter
    query_type = f"{size}_query"
    pipeline_prefix = f"{size}_pipeline"
    
    query_durations = [r.duration for r in results if r.task_type == query_type and r.success]
    pipeline_durations = [r.duration for r in results if r.task_type.startswith(pipeline_prefix) and r.success]
    assert query_durations and pipeline_durations, "No successful tasks found for the specified size."
    
    plt.figure(figsize=(10, 6))
    
    # Plot histograms with transparency
    plt.hist(query_durations, bins=20, alpha=0.6, color="#3498db", 
            label=f'{size.capitalize()} Queries', density=True)
    plt.hist(pipeline_durations, bins=20, alpha=0.6, color="#2ecc71", 
            label=f'{size.capitalize()} Pipelines', density=True)
    
    plt.xlabel('Duration (seconds)')
    plt.ylabel('Density')
    plt.title(f'Durations {size.capitalize()} Queries vs {size.capitalize()} Pipelines')
    plt.legend()
    plt.grid(True, alpha=0.3)
    
    # Save the distribution chart
    plt.savefig(filename, bbox_inches='tight')
    print(f"\nDistribution chart saved to {filename}")
    plt.close()

    return


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
            status = "SUCCESS" if result.success else f"FAILED ({result.error}"
            print(f"[{threading.current_thread().name}] {task_id} completed - {status}, {result.duration:.2f} seconds")
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
    
    # Create distribution chart for small queries vs small pipelines
    distribution_filename = chart_file_path.replace('.png', '_distribution.png')
    draw_query_vs_pipeline_duration(results, distribution_filename, size="small")
    
    print(f"\n\nDone at {datetime.now()}\n\nSee you, space cowboy!")
    return
    
    
if __name__ == "__main__":
    # add some arguments
    import argparse
    parser = argparse.ArgumentParser(description="Simulate load for scheduling test.")
    parser.add_argument('--bpln_profile', default='default')
    parser.add_argument('--seed', type=int, default=42, help='Random seed for reproducibility')
    parser.add_argument('--num_threads', type=int, default=15, help='Number of threads to use for simulation')
    parser.add_argument('--num_tasks', type=int, default=100, help='Number of tasks to simulate')
    parser.add_argument('--chart_file_path', type=str, default='task_chart.png', help='Output path for Gantt chart')
    args = parser.parse_args()
    # now start the main function
    simulate_load(
        bpln_profile=args.bpln_profile,
        seed=args.seed,
        num_threads=args.num_threads,
        num_tasks=args.num_tasks,
        chart_file_path=args.chart_file_path
    )