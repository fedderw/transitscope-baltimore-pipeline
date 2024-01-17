import datetime as dt
from unittest.mock import Mock

import pandas as pd
from prefect import flow

from prefect_transitscope_baltimore_pipeline.tasks import (
    calculate_days_in_month,
    computeCsvStringFromTable,
    format_bus_routes,
    goodbye_prefect_transitscope_baltimore_pipeline,
    hello_prefect_transitscope_baltimore_pipeline,
    standardize_column_names,
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


evaluation_string = r"""(tableSelector, shouldIncludeRowHeaders) => {
    const table = document.querySelector(tableSelector);
    if (!table) {
        return null;
    }

    let csvString = "";
    for (let i = 0; i < table.rows.length; i++) {
        const row = table.rows[i];

        if (!shouldIncludeRowHeaders && i === 0) {
            continue;
        }

        for (let j = 0; j < row.cells.length; j++) {
            const cell = row.cells[j];
            const formattedCellText = cell.innerText.replace(/\n/g, '\n').trim();
            if (formattedCellText !== "No Data") {
                csvString += formattedCellText;
            }

            if (j === row.cells.length - 1) {
                csvString += "\n";
            } else {
                csvString += ",";
            }
        }
    }
    return csvString;
}"""


def test_computeCsvStringFromTable_with_headers():
    # Arrange
    mock_page = Mock()
    mock_page.evaluate.return_value = (
        "header1,header2\nrow1col1,row1col2\nrow2col1,row2col2\n"
    )
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
    assert result == "header1,header2\nrow1col1,row1col2\nrow2col1,row2col2\n"


def test_computeCsvStringFromTable_without_headers():
    # Arrange
    mock_page = Mock()
    mock_page.evaluate.return_value = "row1col1,row1col2\nrow2col1,row2col2\n"
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
    assert result == "row1col1,row1col2\nrow2col1,row2col2\n"


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
    bus_routes_str = "CityLink GOLD, CityLink BLUE, regular route"
    # Expected output
    expected_output = "CityLink Gold, CityLink Blue, Regular Route"
    # Assert that bus routes are formatted correctly
    assert format_bus_routes(bus_routes_str) == expected_output


# Test for calculate_days_in_month function
def test_calculate_days_in_month():
    # Test date for February in a leap year
    date_value = dt.datetime(2020, 2, 15)
    # Assert that the correct number of days is calculated
    assert calculate_days_in_month(date_value) == 29
