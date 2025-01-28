import pandas as pd
from prefect import flow, task

@task(task_run_name="Task for {name}")
@task
def custom_task(name: str):
    print(f"Executing task: {name}")

@flow(flow_run_name="Sub flow for {name}")
def subflow(name: str):
    print(f"Executing subflow: {name}")
    custom_task(name=f"{name}_task")

@flow(flow_run_name="Main flow for {category}")
def main_flow(category: str, value: float):
    subflow(name=f"{category}_{value}")

@flow
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
