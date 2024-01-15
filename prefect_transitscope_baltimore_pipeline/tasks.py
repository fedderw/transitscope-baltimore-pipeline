"""This is an example tasks module"""
from prefect import task


@task
def hello_prefect_transitscope_baltimore_pipeline() -> str:
    """
    Sample task that says hello!

    Returns:
        A greeting for your collection
    """
    return "Hello, prefect-transitscope-baltimore-pipeline!"


@task
def goodbye_prefect_transitscope_baltimore_pipeline() -> str:
    """
    Sample task that says goodbye!

    Returns:
        A farewell for your collection
    """
    return "Goodbye, prefect-transitscope-baltimore-pipeline!"
