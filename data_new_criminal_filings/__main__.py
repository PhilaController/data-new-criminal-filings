from pathlib import Path

import click
import pandas as pd
from loguru import logger
from phl_courts_scraper.new_filings import NewFilingsScraper

from . import DATA_DIR


@click.group()
@click.version_option()
def cli():
    """Processing data for the New Criminal Filings."""
    pass


@cli.command()
def update():
    """Scrape the latest data and update the local data files."""
    # Initialize the scraper and get the data
    scraper = NewFilingsScraper()
    data = scraper().to_pandas()

    # Format date
    data["filing_date"] = pd.to_datetime(data["filing_date"])

    # Save latest dat
    SORT_COLUMNS = ["filing_date", "docket_number", "defendant_name"]
    data.sort_values(SORT_COLUMNS).to_csv(
        DATA_DIR / "raw" / "latest-data.csv", index=False
    )

    # Save combined database
    path = DATA_DIR / "processed" / "daily-data-historical.csv"
    filename = Path(path)

    # Merge together
    if filename.exists():
        data = pd.concat([data, pd.read_csv(filename, parse_dates=["filing_date"])])

    # Remove duplicates and save
    original_length = len(data)
    data = data.drop_duplicates()
    logger.info(f"Removed {original_length - len(data)} duplicate filings")

    # Save
    data.sort_values(SORT_COLUMNS).to_csv(filename, index=False)
