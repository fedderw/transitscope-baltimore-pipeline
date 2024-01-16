"""This is an example flows module"""
from prefect import flow
import asyncio
from prefect_transitscope_baltimore_pipeline.tasks import scrape



# @flow
# def hello_and_goodbye():
#     """
#     Sample flow that says hello and goodbye!
#     """
#     TransitscopebaltimorepipelineBlock.seed_value_for_example()
#     block = TransitscopebaltimorepipelineBlock.load("sample-block")

#     print(hello_prefect_transitscope_baltimore_pipeline())
#     print(f"The block's value: {block.value}")
#     print(goodbye_prefect_transitscope_baltimore_pipeline())
#     return "Done"

@flow
def scrape_and_compute_csv():
    # Executing the main function
    # asyncio.get_event_loop().run_until_complete(scrape())
    scrape()


if __name__ == "__main__":
    # hello_and_goodbye()
    # asyncio.run(scrape_and_compute_csv())
    scrape_and_compute_csv()
    asyncio.get_event_loop().run_until_complete(scrape_and_compute_csv())
