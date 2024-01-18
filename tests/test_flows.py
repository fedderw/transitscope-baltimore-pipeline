import asyncio
from unittest.mock import patch

import pandas as pd
import pytest

from prefect_transitscope_baltimore_pipeline.flows import (
    hello_and_goodbye,
    mta_bus_stops_flow,
    scrape_and_transform_bus_route_ridership,
)
from prefect_transitscope_baltimore_pipeline.tasks import (
    download_mta_bus_stops,
    transform_mta_bus_stops,
)


def test_hello_and_goodbye_flow():
    result = hello_and_goodbye()
    assert result == "Done"


@patch("prefect_transitscope_baltimore_pipeline.flows.scrape")
@patch(
    "prefect_transitscope_baltimore_pipeline.flows.standardize_column_names_task"
)
@patch("prefect_transitscope_baltimore_pipeline.flows.format_bus_routes_task")
@patch(
    "prefect_transitscope_baltimore_pipeline.flows.convert_date_and_calculate_end_of_month"
)
@patch("prefect_transitscope_baltimore_pipeline.flows.exclude_zero_ridership")
@patch(
    "prefect_transitscope_baltimore_pipeline.flows.calculate_days_and_daily_ridership"
)
def test_scrape_and_transform_bus_route_ridership(
    mock_calculate_days_and_daily_ridership,
    mock_exclude_zero_ridership,
    mock_convert_date_and_calculate_end_of_month,
    mock_format_bus_routes_task,
    mock_standardize_column_names_task,
    mock_scrape,
):
    # Arrange
    mock_scrape.return_value = asyncio.Future()
    mock_scrape.return_value.set_result(pd.DataFrame())
    mock_standardize_column_names_task.return_value = pd.DataFrame()
    mock_format_bus_routes_task.return_value = pd.DataFrame()
    mock_convert_date_and_calculate_end_of_month.return_value = pd.DataFrame()
    mock_exclude_zero_ridership.return_value = pd.DataFrame()
    mock_calculate_days_and_daily_ridership.return_value = pd.DataFrame()

    # Act
    asyncio.run(scrape_and_transform_bus_route_ridership())

    # Assert
    mock_scrape.assert_called_once()
    mock_standardize_column_names_task.assert_called_once()
    mock_format_bus_routes_task.assert_called_once()
    mock_convert_date_and_calculate_end_of_month.assert_called_once()
    mock_exclude_zero_ridership.assert_called_once()
    mock_calculate_days_and_daily_ridership.assert_called_once()


# Creating a mock function for download_mta_bus_stops
@pytest.fixture
def mock_download_mta_bus_stops(monkeypatch):
    def mock(*args, **kwargs):
        # Mocked data, can be adjusted as per requirement
        return "Mocked data for MTA bus stops"

    monkeypatch.setattr(download_mta_bus_stops, "run", mock)


# Creating a mock function for transform_mta_bus_stops
@pytest.fixture
def mock_transform_mta_bus_stops(monkeypatch):
    def mock(*args, **kwargs):
        # Mocked data, can be adjusted as per requirement
        return "Transformed mocked data for MTA bus stops"

    monkeypatch.setattr(transform_mta_bus_stops, "run", mock)


# Unit test for the flow
def test_mta_bus_stops_flow(
    mock_download_mta_bus_stops, mock_transform_mta_bus_stops
):
    # Run the flow
    result = mta_bus_stops_flow()

    # Test assertions
    assert (
        result == "Transformed mocked data for MTA bus stops"
    ), "The flow did not return the expected result."
