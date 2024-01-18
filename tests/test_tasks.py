import asyncio
import datetime as dt
from datetime import datetime
from unittest.mock import Mock

import geopandas as gpd
import pandas as pd
import pytest
from prefect import flow
from shapely.geometry import Point

from prefect_transitscope_baltimore_pipeline.tasks import (
    calculate_days_in_month,
    computeCsvStringFromTable,
    download_mta_bus_stops,
    evaluation_string,
    format_bus_routes,
    goodbye_prefect_transitscope_baltimore_pipeline,
    hello_prefect_transitscope_baltimore_pipeline,
    standardize_column_names,
    transform_mta_bus_stops,
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


@pytest.mark.asyncio
async def test_computeCsvStringFromTable_with_headers():
    # Arrange
    mock_page = Mock()
    # Create a Future object
    future = asyncio.Future()

    # Set the result of the Future. This is what will be returned when the Future is awaited.
    future.set_result(
        r"header1,header2\nrow1col1,row1col2\nrow2col1,row2col2\n"
    )

    # Now, when the mock_page.evaluate function is called, it will return the Future.
    mock_page.evaluate.return_value = future
    table_selector = "#table"
    should_include_row_headers = True

    # Act
    result = await computeCsvStringFromTable(
        mock_page, table_selector, should_include_row_headers
    )

    # Assert
    mock_page.evaluate.assert_called_once_with(
        evaluation_string,  # The JavaScript function is the docstring of the Python function
        table_selector,
        should_include_row_headers,
    )
    assert result == r"header1,header2\nrow1col1,row1col2\nrow2col1,row2col2\n"


@pytest.mark.asyncio
async def test_computeCsvStringFromTable_without_headers():
    # Arrange
    mock_page = Mock()
    # Create a Future object
    future = asyncio.Future()

    # Set the result of the Future. This is what will be returned when the Future is awaited.
    future.set_result(r"row1col1,row1col2\nrow2col1,row2col2\n")

    # Now, when the mock_page.evaluate function is called, it will return the Future.
    mock_page.evaluate.return_value = future
    table_selector = "#table"
    should_include_row_headers = False

    # Act
    result = await computeCsvStringFromTable(
        mock_page, table_selector, should_include_row_headers
    )

    # Assert
    mock_page.evaluate.assert_called_once_with(
        evaluation_string,  # The JavaScript function is the docstring of the Python function
        table_selector,
        should_include_row_headers,
    )
    assert result == r"row1col1,row1col2\nrow2col1,row2col2\n"


# Test for standardize_column_names function
def test_standardize_column_names():
    # Create a sample DataFrame
    data = {"First Name": ["Alice", "Bob"], "Last Name": ["Smith", "Jones"]}
    df = pd.DataFrame(data)
    # Standardize column names
    standardized_df = standardize_column_names(df)
    # Assert that column names are standardized correctly
    assert list(standardized_df.columns) == ["first_name", "last_name"]


# Test for format_bus_routes function
def test_format_bus_routes():
    # Test string for bus routes
    bus_routes_str = "CityLink GOLD, CityLink BLUE, 100"
    # Expected output
    expected_output = "CityLink Gold, CityLink Blue, 100"
    # Assert that bus routes are formatted correctly
    assert format_bus_routes(bus_routes_str) == expected_output


# Test for calculate_days_in_month function
def test_calculate_days_in_month():
    # Test date for February in a leap year
    date_value = dt.datetime(2020, 2, 15)
    # Assert that the correct number of days is calculated
    assert calculate_days_in_month(date_value) == 29


# Mocks
class MockResponse:
    def __init__(self, json_data, status_code):
        self.json_data = json_data
        self.status_code = status_code

    def json(self):
        return self.json_data


@pytest.fixture
def mock_requests_get(monkeypatch):
    def mock_get(*args, **kwargs):
        return MockResponse({"description": "Successful description"}, 200)

    monkeypatch.setattr("requests.get", mock_get)


@pytest.fixture
def mock_requests_get_failure(monkeypatch):
    def mock_get(*args, **kwargs):
        return MockResponse(None, 404)

    monkeypatch.setattr("requests.get", mock_get)


def test_download_mta_bus_stops_success(mock_requests_get):
    # Remember to add .fn
    result = download_mta_bus_stops.fn()
    first_description = result["data_source_description"].values[0]
    assert first_description != "No description available"


def test_download_mta_bus_stops_failure(mock_requests_get_failure):
    result = download_mta_bus_stops.fn()
    first_description = result["data_source_description"].values[0]
    assert first_description == "No description available"


# Tests for transform_mta_bus_stops function
def test_transform_mta_bus_stops():
    test_gdf = gpd.GeoDataFrame(
        {
            "objectid": [1],
            "stop_name": ["Test Stop"],
            "rider_on": [169.0],
            "rider_off": [127.0],
            "rider_total": [297.0],
            "stop_ridership_rank": [264.0],
            "routes_served": ["CityLink Gold, BL, 100"],
            "distribution_policy": ["E1 - Public Domain - Internal Use Only"],
            "mode": ["Bus"],
            "shelter": ["Yes"],
            "county": ["Baltimore City"],
            "stop_id": [1],
            "geometry": [Point(1, 2)],  # Mock geometry points
        }
    )
    result = transform_mta_bus_stops.fn(test_gdf)
    assert all(
        x in result.columns for x in ["latitude", "longitude", "routes_served"]
    )
