from datetime import date
from pathlib import Path

import numpy as np
import pandas as pd
import requests
from bs4 import BeautifulSoup
from loguru import logger

URL = "https://www.courts.phila.gov/NewCriminalFilings/date/default.aspx"
CWD = Path(__file__).resolve().parent
DATA_DIR = CWD / ".." / "data"


def get_all_pages(date):
    """For the specific date, get all page URLs."""
    r = requests.get(URL, params={"search": date})
    soup = BeautifulSoup(r.text, "html.parser")

    return [
        f"https://www.courts.phila.gov/{url}"
        for url in set([a["href"] for a in soup.select(".pagination li a")])
    ]


def parse_single_page(url):
    """For the input url, extract the data from the page."""

    # Request the html and parse with beautiful soup
    r = requests.get(url)
    soup = BeautifulSoup(r.text, "html.parser")

    # The output results
    data = []

    # Loop over each row
    for row in soup.select(".panel-body .row"):

        # Save the result for this row
        result = {}

        # Loop over each column in the row
        for col in row.select(".col-md-4 p"):

            # Extract keys/values in this column
            keys = []
            values = []
            for x in col:
                s = str(x).strip()

                # Keys are in bold
                if "strong" in s:
                    keys.append(x.text.strip(":"))
                # Skip any line breaks
                elif "br" in s:
                    continue
                else:
                    # If we have text, its the value
                    if s:
                        if hasattr(x, "text"):
                            s = x.text
                        values.append(s)
            # Update the result dict for this row
            result.update(dict(zip(keys, values)))

        # Save row results
        data.append(result)

    # Return a dataframe
    return pd.DataFrame(data)


if __name__ == "__main__":

    # Determine the allowed date range, e.g., the last week
    today = date.today()
    allowed_dates = [str(today - pd.Timedelta(i, "D")) for i in range(8)]

    # Get data from all pages for all dates
    data = pd.concat(
        map(
            lambda date: pd.concat(map(parse_single_page, get_all_pages(date))),
            allowed_dates,
        ),
        ignore_index=True,
    ).replace("None", np.nan)

    # Save raw data
    data.to_csv(DATA_DIR / "raw" / "latest-data.csv", index=False)
    logger.info(f"Successfully scraped data for {len(data)} criminal filings")

    # Save combined database
    path = DATA_DIR / "processed" / "daily-data-combined.csv"
    filename = Path(path)

    # Merge together
    if filename.exists():
        data = pd.concat([data, pd.read_csv(filename)])

    # Remove duplicates and save
    data = data.drop_duplicates()
    data.to_csv(filename, index=False)
