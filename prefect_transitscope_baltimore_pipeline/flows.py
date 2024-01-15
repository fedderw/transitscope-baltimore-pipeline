"""This is an example flows module"""
from prefect import flow

from prefect_transitscope_baltimore_pipeline.blocks import TransitscopebaltimorepipelineBlock
from prefect_transitscope_baltimore_pipeline.tasks import (
    goodbye_prefect_transitscope_baltimore_pipeline,
    hello_prefect_transitscope_baltimore_pipeline,
)


@flow
def hello_and_goodbye():
    """
    Sample flow that says hello and goodbye!
    """
    TransitscopebaltimorepipelineBlock.seed_value_for_example()
    block = TransitscopebaltimorepipelineBlock.load("sample-block")

    print(hello_prefect_transitscope_baltimore_pipeline())
    print(f"The block's value: {block.value}")
    print(goodbye_prefect_transitscope_baltimore_pipeline())
    return "Done"


if __name__ == "__main__":
    hello_and_goodbye()
