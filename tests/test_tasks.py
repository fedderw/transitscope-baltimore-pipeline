from prefect import flow

from prefect_transitscope_baltimore_pipeline.tasks import (
    goodbye_prefect_transitscope_baltimore_pipeline,
    hello_prefect_transitscope_baltimore_pipeline,
)


def test_hello_prefect_transitscope_baltimore_pipeline():
    @flow
    def test_flow():
        return hello_prefect_transitscope_baltimore_pipeline()

    result = test_flow()
    assert result == "Hello, prefect-transitscope-baltimore-pipeline!"


def goodbye_hello_prefect_transitscope_baltimore_pipeline():
    @flow
    def test_flow():
        return goodbye_prefect_transitscope_baltimore_pipeline()

    result = test_flow()
    assert result == "Goodbye, prefect-transitscope-baltimore-pipeline!"
