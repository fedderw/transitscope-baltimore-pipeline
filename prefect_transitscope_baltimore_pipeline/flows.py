"""This is an example flows module"""
import asyncio
from pathlib import Path

import boto3
from prefect import flow
from prefect.blocks.system import Secret

from prefect_transitscope_baltimore_pipeline.tasks import (
    calculate_days_and_daily_ridership,
    convert_date_and_calculate_end_of_month,
    download_mta_bus_stops,
    exclude_zero_ridership,
    format_bus_routes_task,
    goodbye_prefect_transitscope_baltimore_pipeline,
    hello_prefect_transitscope_baltimore_pipeline,
    scrape,
    standardize_column_names_task,
    transform_mta_bus_stops,
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

    # Write parquet file to local directory
    bus_ridership_data.to_parquet("data/mta_bus_ridership.parquet")
    return bus_ridership_data


@flow
async def upload_mta_bus_ridership_to_s3():
    """
    This function uploads the MTA bus ridership data to an S3 bucket asynchronously.
    """
    aws_access_key_id_block = await Secret.load("aws-access-key-id")
    # Access the stored secret
    aws_access_key_id = aws_access_key_id_block.get()
    aws_secret_access_key_block = await Secret.load("aws-secret-access-key")
    # Access the stored secret
    aws_secret_access_key = aws_secret_access_key_block.get()

    session = boto3.Session(
        aws_access_key_id=aws_access_key_id,
        aws_secret_access_key=aws_secret_access_key,
    )

    s3 = session.resource("s3")
    path = Path("data/mta_bus_ridership.parquet")
    # Upload the parquet file to the S3 bucket
    s3.meta.client.upload_file(
        Filename=str(path),
        Bucket="transitscope-baltimore",
        Key="data/mta_bus_ridership.parquet",
    )


@flow
def mta_bus_stops_flow():
    """
    Flow to process MTA bus stops data.
    """
    # First task to download MTA bus stops data
    stops = download_mta_bus_stops()

    # Second task to transform the MTA bus stops data
    transformed_stops = transform_mta_bus_stops(stops)
    transformed_stops.to_parquet("data/mta_bus_stops.parquet")
    print("MTA bus stops data processing complete.")
    return transformed_stops


@flow
async def upload_mta_bus_stops_to_s3():
    aws_access_key_id_block = await Secret.load("aws-access-key-id")
    # Access the stored secret
    aws_access_key_id = aws_access_key_id_block.get()
    aws_secret_access_key_block = await Secret.load("aws-secret-access-key")
    # Access the stored secret
    aws_secret_access_key = aws_secret_access_key_block.get()

    session = boto3.Session(
        aws_access_key_id=aws_access_key_id,
        aws_secret_access_key=aws_secret_access_key,
    )

    s3 = session.resource("s3")
    path = Path("data/mta_bus_stops.parquet")
    # Upload the parquet file to the S3 bucket
    s3.meta.client.upload_file(
        Filename=str(path),
        Bucket="transitscope-baltimore",
        Key="data/mta_bus_stops.parquet",
    )


if __name__ == "__main__":
    asyncio.run(scrape_and_transform_bus_route_ridership())
    asyncio.run(upload_mta_bus_ridership_to_s3())
    mta_bus_stops_flow()
    asyncio.run(upload_mta_bus_stops_to_s3())
