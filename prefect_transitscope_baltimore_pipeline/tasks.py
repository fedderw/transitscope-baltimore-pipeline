"""This is an example tasks module"""
import asyncio
import calendar
import datetime as dt
import re
from datetime import datetime
from io import StringIO

import geopandas as gpd
import pandas as pd
import requests
from prefect import task
from pyppeteer import launch
from tqdm import tqdm


@task
def hello_prefect_transitscope_baltimore_pipeline() -> str:
    """
    Sample task that says hello!

    Returns:
        A greeting for your collection
    """
    return "Hello, prefect-transitscope-baltimore-pipeline!"


@task
def goodbye_prefect_transitscope_baltimore_pipeline() -> str:
    """
    Sample task that says goodbye!

    Returns:
        A farewell for your collection
    """
    return "Goodbye, prefect-transitscope-baltimore-pipeline!"


# -------------------------------------------------------- #
#   Scrape the route ridership data from the MTA website   #
# -------------------------------------------------------- #
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


async def computeCsvStringFromTable(
    page, tableSelector, shouldIncludeRowHeaders
):
    # Extracting CSV string from a table element
    csvString = await page.evaluate(
        evaluation_string,
        tableSelector,
        shouldIncludeRowHeaders,
    )

    return csvString


@task
async def scrape():
    # Launching the browser and setting up a new page
    browser = await launch(
        handleSIGINT=False, handleSIGTERM=False, handleSIGHUP=False
    )

    page = await browser.newPage()
    await page.setViewport({"width": 1920, "height": 1080})
    await page.goto("https://www.mta.maryland.gov/performance-improvement")

    # Interacting with elements on the page
    await page.click("h3#ui-id-5")
    csvString = ""

    # Selecting and processing data from dropdown options
    routeSelectSelector = 'select[name="ridership-select-route"]'
    routeSelectOptions = await page.evaluate(
        """() => Array.from(document.querySelectorAll('select[name="ridership-select-route"] option')).map(option => option.value)"""
    )
    print(f"Route select options: {routeSelectOptions}")

    monthSelectSelector = 'select[name="ridership-select-month"]'
    monthSelectOptions = await page.evaluate(
        """() => Array.from(document.querySelectorAll('select[name="ridership-select-month"] option')).map(option => option.value)"""
    )
    print(f"Month select options: {monthSelectOptions}")

    yearSelectSelector = 'select[name="ridership-select-year"]'
    yearSelectOptions = await page.evaluate(
        """() => Array.from(document.querySelectorAll('select[name="ridership-select-year"] option')).map(option => option.value)"""
    )
    print(f"Year select options: {yearSelectOptions}")
    # Now, we need to click on the 'submit' button to get the data
    # example: <button class="btn btn-default btn-submit btn-ridership" type="submit" name="submit">Submit</button>
    # await page.click('button.btn.btn-default.btn-submit.btn-ridership')

    # Looping through options to generate CSV data
    hasIncludedRowHeaders = True
    for yearSelectOption in tqdm(
        yearSelectOptions, position=0, leave=False, desc="Years"
    ):
        await page.focus(yearSelectSelector)
        await page.select(yearSelectSelector, yearSelectOption)

        for monthSelectOption in tqdm(
            monthSelectOptions, position=1, leave=False, desc="Months"
        ):
            await page.focus(monthSelectSelector)
            await page.select(monthSelectSelector, monthSelectOption)
            # Printing the selected option
            # tqdm.write(f"Selected month: {monthSelectOption}")
            await page.keyboard.press("Tab")
            await page.keyboard.press("Tab")

            # Waiting for network responses after form submission
            await asyncio.gather(
                # page.keyboard.press('Enter'),
                page.click("button.btn.btn-default.btn-submit.btn-ridership"),
                # Wait for 1 second after clicking the button
                page.waitFor(500),
                # page.waitForNavigation(),
                # page.waitForNavigation({'waitUntil': 'networkidle2', 'timeout': 30000})s
            )
            # navigationPromise = async.ensure_future(page.waitForNavigation())
            # await page.click('a.my-link')  # indirectly cause a navigation
            # await navigationPromise  # wait until navigation finishes
            # Concatenating CSV string for each selection
            csvString += await computeCsvStringFromTable(
                page,
                "div#container-ridership-table > table",
                hasIncludedRowHeaders,
            )

            if hasIncludedRowHeaders:
                hasIncludedRowHeaders = False

    # Closing the browser
    await browser.close()

    # Converting the CSV string to a pandas dataframe
    df = pd.read_csv(StringIO(csvString))

    # Writing the CSV string to a file
    # with open("mta_bus_ridership.csv", "w") as file:
    #     file.write(csvString)

    return df


# -------------- Transform the scraped data -------------- #
def standardize_column_names(data_frame):
    """Standardize DataFrame column names to lowercase with underscores."""
    data_frame.columns = (
        data_frame.columns.str.strip().str.lower().str.replace(" ", "_")
    )
    return data_frame


def format_bus_routes(bus_routes_str):
    """Format bus route strings, capitalizing CityLink routes."""
    citylink_pattern = re.compile("CityLink ([A-Z]+)")
    formatted_routes = [
        citylink_pattern.sub(
            lambda match: "CityLink " + match.group(1).title(),
            route.strip(),
        )
        for route in bus_routes_str.split(",")
    ]
    return ", ".join(formatted_routes)


def calculate_days_in_month(date_value):
    """Calculate the number of days in a given month."""
    last_day_of_month = dt.datetime(
        date_value.year,
        date_value.month,
        calendar.monthrange(date_value.year, date_value.month)[1],
    )
    return last_day_of_month.day


@task
def standardize_column_names_task(data_frame):
    """Task to standardize DataFrame column names to lowercase with underscores."""
    return standardize_column_names(data_frame)


@task
def format_bus_routes_task(bus_ridership_data):
    """Task to format bus route strings, capitalizing CityLink routes."""
    bus_ridership_data["route"] = bus_ridership_data["route"].apply(
        format_bus_routes
    )
    return bus_ridership_data


@task
def convert_date_and_calculate_end_of_month(bus_ridership_data):
    """Task to convert date column to datetime format and calculate end date of month."""
    bus_ridership_data["date"] = pd.to_datetime(
        bus_ridership_data["date"], format="%m/%Y"
    )
    bus_ridership_data["end_of_month_date"] = bus_ridership_data[
        "date"
    ] + pd.offsets.MonthEnd(0)
    return bus_ridership_data


@task
def exclude_zero_ridership(bus_ridership_data):
    """Task to exclude rows with zero ridership."""
    bus_ridership_data = bus_ridership_data[
        bus_ridership_data["ridership"] > 0
    ]
    return bus_ridership_data


@task
def calculate_days_and_daily_ridership(bus_ridership_data):
    """Task to calculate number of days in the month and daily ridership."""
    bus_ridership_data["days_in_month"] = bus_ridership_data["date"].apply(
        calculate_days_in_month
    )
    bus_ridership_data["daily_ridership"] = (
        bus_ridership_data["ridership"] / bus_ridership_data["days_in_month"]
    )
    return bus_ridership_data


# -------------------------------------------------------- #
#    SECTION: Request the MTA bus stop data from the API   #
# -------------------------------------------------------- #

# Dictionary mapping short color codes to their corresponding CityLink route names
color_to_citylink = {
    "BL": "CityLink Blue",
    "BR": "CityLink Brown",
    "CityLink BLUE": "CityLink Blue",
    "CityLink NAVY": "CityLink Navy",
    "CityLink ORANGE": "CityLink Orange",
    "CityLink RED": "CityLink Red",
    "CityLink SILVER": "CityLink Silver",
    "GD": "CityLink Gold",
    "GR": "CityLink Green",
    "LM": "CityLink Lime",
    "NV": "CityLink Navy",
    "OR": "CityLink Orange",
    "PK": "CityLink Pink",
    "PR": "CityLink Purple",
    "RD": "CityLink Red",
    "SV": "CityLink Silver",
    "YW": "CityLink Yellow",
}


# Function to map colors to their full names, keeping unmatched values
def map_color_to_citylink(color):
    """
    The function `map_color_to_citylink` maps a color to a corresponding citylink or returns the color
    itself if no mapping is found.

    :param color: The color parameter is a string representing a color
    :return: the value associated with the given color in the `color_to_citylink` dictionary. If the
    color is not found in the dictionary, it will return the color itself.
    """
    return color_to_citylink.get(color, color)


# Function to download MTA bus stops data
@task
def download_mta_bus_stops():
    metadata_url = "https://geodata.md.gov/imap/rest/services/Transportation/MD_Transit/FeatureServer/9?f=pjson"
    metadata_response = requests.get(metadata_url)
    if metadata_response.status_code == 200:
        description = metadata_response.json().get(
            "description", "No description available"
        )
        print("Description from Metadata:", description)
    else:
        print("Failed to retrieve metadata")
        description = "No description available"

    stops = gpd.read_file(
        "https://geodata.md.gov/imap/rest/services/Transportation/MD_Transit/FeatureServer/9/query?where=1%3D1&outFields=*&outSR=4326&f=geojson"
    )
    stops = standardize_column_names(stops)
    stops["data_source_description"] = description
    stops["download_date"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    return stops


@task
def transform_mta_bus_stops(gdf):
    gdf["latitude"] = gdf["geometry"].y
    gdf["longitude"] = gdf["geometry"].x
    route_stop = gdf[["stop_id", "routes_served"]].copy()

    # We need to split on commas and semicolons
    route_stop["routes_served"] = route_stop["routes_served"].str.split(",")
    route_stop = route_stop.explode("routes_served")
    # Split on semicolons
    route_stop["routes_served"] = route_stop["routes_served"].str.split(";")
    route_stop = route_stop.explode("routes_served")
    route_stop["routes_served"] = route_stop["routes_served"].str.strip()
    # Apply the function to the 'routes_served' column
    route_stop["routes_served"] = route_stop["routes_served"].apply(
        map_color_to_citylink
    )
    # Re-join the routes served by stop into a df with one row per stop: route_stop, routes_served
    route_stop = (
        route_stop.groupby("stop_id")["routes_served"]
        .apply(list)
        .reset_index()
    )
    # Remove the brackets from the list of routes served
    route_stop["routes_served"] = route_stop["routes_served"].str.strip("[]")
    # Drop routes_served from the original gdf
    gdf = gdf.drop(columns=["routes_served"])
    # Merge the routes served by stop back into the original gdf
    gdf = gdf.merge(route_stop, on="stop_id", how="left")
    # Shift routes_served to position 6
    gdf.insert(6, "routes_served", gdf.pop("routes_served"))
    return gdf
