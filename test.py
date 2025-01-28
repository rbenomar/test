import pandas as pd
from prefect import flow, task
from datetime import timedelta
from datetime import datetime

def generate_flow_run_name():
    return f"Custom run {datetime.now().strftime('%Y%m%d%H%M%S')}"

@task(
    task_run_name="Task for {name}",
    log_prints=True,
    cache_key_fn=lambda context, parameters: f"custom_task-{parameters['name']}",
    cache_expiration=timedelta(minutes=5)
)
def custom_task(name: str):
    print(f"Executing task: {name}")
    return f"Result from {name}"

@flow(flow_run_name="Sub flow for {name}", 
      log_prints=True, 
      persist_result=True)
def subflow(name: str):
    print(f"Executing subflow: {name}")
    custom_task(name=f"{name}_task")
    result = custom_task(name=f"{name}_task")
    return result

@flow(flow_run_name="Main flow for {category}", log_prints=True)
def main_flow(category: str, value: float):
    print(f"Executing main flow for {category} with value: {value}")    
    result = subflow(name=f"{category}_{value}")
    print(f"Result from subflow: {result}")

@flow(flow_run_name=generate_flow_run_name, log_prints=True)
def parent_flow():
    # Create a sample dataset
    df = pd.DataFrame({
        'category': ['A', 'B', 'C', 'D'],
        'value': [1.5, 2.7, 3.2, 4.8]
    })

    # Iterate through the dataframe
    for index, row in df.iterrows():
        main_flow(category=row['category'], value=row['value'])

if __name__ == "__main__":
    parent_flow()

# prefect deploy test.py:parent_flow -n "DataFrameIteration" -p test


#   After creating the deployment, you can run it using:

# prefect deployment run "parent_flow/DataFrameIteration"


#  If you want to schedule the deployment to run periodically, you can add a schedule when creating the deployment:
# prefect deploy test.py:parent_flow -n "DataFrameIteration" -p default-agent-pool --cron "0 0 * * *"
