"""This is an example flows module"""
import asyncio

from prefect import flow

from prefect_transitscope_baltimore_pipeline.tasks import (
    calculate_days_and_daily_ridership,
    convert_date_and_calculate_end_of_month,
    exclude_zero_ridership,
    format_bus_routes_task,
    goodbye_prefect_transitscope_baltimore_pipeline,
    hello_prefect_transitscope_baltimore_pipeline,
    scrape,
    standardize_column_names_task,
)


@flow
def hello_and_goodbye():
    """
    Sample flow that says hello and goodbye!
    """
    # TransitscopebaltimorepipelineBlock.seed_value_for_example()
    # block = TransitscopebaltimorepipelineBlock.load("sample-block")

    print(hello_prefect_transitscope_baltimore_pipeline())
    # print(f"The block's value: {block.value}")
    print(goodbye_prefect_transitscope_baltimore_pipeline())
    return "Done"


@flow
async def scrape_and_transform_bus_route_ridership():
    # Executing the main function
    bus_ridership_data = await scrape()
    bus_ridership_data = standardize_column_names_task(bus_ridership_data)
    bus_ridership_data = format_bus_routes_task(bus_ridership_data)
    bus_ridership_data = convert_date_and_calculate_end_of_month(
        bus_ridership_data
    )
    bus_ridership_data = exclude_zero_ridership(bus_ridership_data)
    bus_ridership_data = calculate_days_and_daily_ridership(bus_ridership_data)
    print(bus_ridership_data.head())


if __name__ == "__main__":
    # hello_and_goodbye()
    asyncio.run(scrape_and_transform_bus_route_ridership())
