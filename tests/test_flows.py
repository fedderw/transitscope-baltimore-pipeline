import asyncio
from unittest.mock import MagicMock, patch

import pandas as pd
import pytest

from prefect_transitscope_baltimore_pipeline.flows import (
    hello_and_goodbye,
    mta_bus_stops_flow,
    scrape_and_transform_bus_route_ridership,
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


# -------------------------------------------------------- #
#              #SECTION: test mta bus stops flow           #
# -------------------------------------------------------- #


# Mock function returning a simple DataFrame
def mock_df(*args, **kwargs):
    return pd.DataFrame({"mock_column": ["mock_data"]})


@pytest.fixture
def mock_download_mta_bus_stops(monkeypatch):
    mock = MagicMock(side_effect=mock_df)
    monkeypatch.setattr(
        "prefect_transitscope_baltimore_pipeline.tasks.download_mta_bus_stops",
        mock,
    )
    return mock


@pytest.fixture
def mock_transform_mta_bus_stops(monkeypatch):
    mock = MagicMock(side_effect=mock_df)
    monkeypatch.setattr(
        "prefect_transitscope_baltimore_pipeline.tasks.transform_mta_bus_stops",
        mock,
    )
    return mock


def test_mta_bus_stops_flow(
    mock_download_mta_bus_stops, mock_transform_mta_bus_stops
):
    # Run the flow
    result = mta_bus_stops_flow()

    # Test assertions
    assert isinstance(
        result, pd.DataFrame
    ), "The flow did not return a DataFrame."
    assert not result.empty, "The returned DataFrame is empty."
