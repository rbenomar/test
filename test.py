import pandas as pd
from prefect import flow, task
from datetime import datetime

@task(task_run_name="Task for {name}", log_prints=True)
@task
def custom_task(name: str):
    print(f"Executing task: {name}")

@flow(flow_run_name="Sub flow for {name}", log_prints=True)
def subflow(name: str):
    print(f"Executing subflow: {name}")
    custom_task(name=f"{name}_task")

@flow(flow_run_name="Main flow for {category}", log_prints=True)
def main_flow(category: str, value: float):
    print(f"Executing main flow for {category} with value: {value}")    
    subflow(name=f"{category}_{value}")

@flow(flow_run_name="Custom run {timestamp}", log_prints=True)
def parent_flow(timestamp: str):
    # Create a sample dataset
    df = pd.DataFrame({
        'category': ['A', 'B', 'C', 'D'],
        'value': [1.5, 2.7, 3.2, 4.8]
    })

    # Iterate through the dataframe
    for index, row in df.iterrows():
        main_flow(category=row['category'], value=row['value'])

if __name__ == "__main__":
    timestamp = datetime.now().strftime("%Y%m%d%H%M%S")
    parent_flow(timestamp=timestamp)
