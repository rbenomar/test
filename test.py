import pandas as pd
from prefect import flow, task

@task
def custom_task(name: str):
    print(f"Executing task: {name}")

@flow
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
