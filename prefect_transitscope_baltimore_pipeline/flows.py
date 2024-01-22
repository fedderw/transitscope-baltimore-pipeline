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
    scrape,
    standardize_column_names_task,
    transform_mta_bus_stops,
)


@flow
async def scrape_and_transform_bus_route_ridership():
    """
    This is an asynchronous function that scrapes bus ridership data,
    transforms it, and writes it to a parquet file.

    The function performs the following steps:
    1. Scrapes the data
    2. Standardizes the column names
    3. Formats the bus routes
    4. Converts the date and calculates the end of the month
    5. Excludes zero ridership
    6. Calculates the days and daily ridership
    7. Writes the transformed data to a parquet file

    Returns:
        DataFrame: The transformed bus ridership data.
    """
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
    This is an asynchronous function that uploads the MTA bus ridership data to an S3 bucket.

    The function performs the following steps:
    1. Loads the AWS access key ID and secret access key from secrets
    2. Creates a session with AWS using the loaded credentials
    3. Creates an S3 resource object using the session
    4. Uploads the MTA bus ridership data (in parquet format) to the specified S3 bucket

    Returns:
        None
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
    This is an asynchronous function that uploads the MTA bus ridership data to an S3 bucket.

    The function performs the following steps:
    1. Loads the AWS access key ID and secret access key from secrets
    2. Creates a session with AWS using the loaded credentials
    3. Creates an S3 resource object using the session
    4. Uploads the MTA bus ridership data (in parquet format) to the specified S3 bucket

    Returns:
        None
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
    """
    Asynchronous function to upload MTA bus stops data to an S3 bucket.
    """
    aws_access_key_id = await Secret.load("aws-access-key-id").get()
    aws_secret_access_key = await Secret.load("aws-secret-access-key").get()

    session = boto3.Session(
        aws_access_key_id=aws_access_key_id,
        aws_secret_access_key=aws_secret_access_key,
    )

    s3 = session.resource("s3")
    path = Path("data/mta_bus_stops.parquet")
    s3.meta.client.upload_file(
        Filename=path,
        Bucket="transitscope-baltimore",
        Key="data/mta_bus_stops.parquet",
    )


@flow
async def run_all_prefect_transitscope_baltimore_pipeline_flows():
    """
    This is an asynchronous function that runs all the flows in the module.

    The function performs the following steps:
    1. Runs the scrape_and_transform_bus_route_ridership flow
    2. Runs the upload_mta_bus_ridership_to_s3 flow
    3. Runs the mta_bus_stops_flow flow
    4. Runs the upload_mta_bus_stops_to_s3 flow

    Returns:
        None
    """
    await scrape_and_transform_bus_route_ridership()
    await upload_mta_bus_ridership_to_s3()
    mta_bus_stops_flow()
    await upload_mta_bus_stops_to_s3()
    print("All flows completed successfully.")
    return None
    # return scrape_and_transform_bus_route_ridership()
    # return upload_mta_bus_ridership_to_s3()
    # return mta_bus_stops_flow()


if __name__ == "__main__":
    asyncio.run(scrape_and_transform_bus_route_ridership())
    asyncio.run(upload_mta_bus_ridership_to_s3())
    mta_bus_stops_flow()
    asyncio.run(upload_mta_bus_stops_to_s3())
