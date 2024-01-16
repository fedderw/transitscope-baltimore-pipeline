"""This is an example tasks module"""
from prefect import task
import asyncio
from pyppeteer import launch
from tqdm import tqdm
import pandas as pd
from io import StringIO


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



async def computeCsvStringFromTable(page, tableSelector, shouldIncludeRowHeaders):
    # Extracting CSV string from a table element
    csvString = await page.evaluate(r'''(tableSelector, shouldIncludeRowHeaders) => {
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
    }''', tableSelector, shouldIncludeRowHeaders)

    return csvString

@task
async def scrape():
    # Launching the browser and setting up a new page
    browser = await launch()
    page = await browser.newPage()
    await page.setViewport({'width': 1920, 'height': 1080})
    await page.goto('https://www.mta.maryland.gov/performance-improvement')

    # Interacting with elements on the page
    await page.click('h3#ui-id-5')
    csvString = ""

    # Selecting and processing data from dropdown options
    routeSelectSelector = 'select[name="ridership-select-route"]'
    routeSelectOptions = await page.evaluate('''() => Array.from(document.querySelectorAll('select[name="ridership-select-route"] option')).map(option => option.value)''')
    print(f"Route select options: {routeSelectOptions}")


    monthSelectSelector = 'select[name="ridership-select-month"]'
    monthSelectOptions = await page.evaluate('''() => Array.from(document.querySelectorAll('select[name="ridership-select-month"] option')).map(option => option.value)''')
    print(f"Month select options: {monthSelectOptions}")

    yearSelectSelector = 'select[name="ridership-select-year"]'
    yearSelectOptions = await page.evaluate('''() => Array.from(document.querySelectorAll('select[name="ridership-select-year"] option')).map(option => option.value)''')
    print(f"Year select options: {yearSelectOptions}")
    # Now, we need to click on the 'submit' button to get the data
    # example: <button class="btn btn-default btn-submit btn-ridership" type="submit" name="submit">Submit</button>
    # await page.click('button.btn.btn-default.btn-submit.btn-ridership')
    

    # Looping through options to generate CSV data
    hasIncludedRowHeaders = True
    for yearSelectOption in tqdm(yearSelectOptions):
        await page.focus(yearSelectSelector)
        await page.select(yearSelectSelector, yearSelectOption)
        # Printing the selected option
        # print(f"Selected year: {yearSelectOption}")

        for monthSelectOption in tqdm(monthSelectOptions):
            await page.focus(monthSelectSelector)
            await page.select(monthSelectSelector, monthSelectOption)
            # Printing the selected option
            # print(f"Selected month: {monthSelectOption}")
            await page.keyboard.press("Tab")
            await page.keyboard.press("Tab")

            # Waiting for network responses after form submission
            await asyncio.gather(
                # page.keyboard.press('Enter'),
                page.click('button.btn.btn-default.btn-submit.btn-ridership'),
                # Wait for 1 second after clicking the button
                page.waitFor(500),
                # page.waitForNavigation(),
                # page.waitForNavigation({'waitUntil': 'networkidle2', 'timeout': 30000})  # 30 seconds
                # Just waiting for navigation is not enough, as the page may not be fully loaded. To 
            )
            # navigationPromise = async.ensure_future(page.waitForNavigation())
            # await page.click('a.my-link')  # indirectly cause a navigation
            # await navigationPromise  # wait until navigation finishes




            # Concatenating CSV string for each selection
            csvString += await computeCsvStringFromTable(page, 'div#container-ridership-table > table', hasIncludedRowHeaders)

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
 

# Executing the main function
# asyncio.get_event_loop().run_until_complete(scrape())
