import pandas as pd
from phl_courts_scraper.new_filings import NewFilingsScraper

from . import DATA_DIR


def load_historical_data():
    """Load historical data."""

    filings = pd.read_csv(DATA_DIR / "processed" / "daily-data-historical.csv")
    portal_results = pd.read_csv(
        DATA_DIR / "processed" / "portal-results-historical.csv"
    )

    # Merge together
    return filings.merge(
        portal_results,
        on="docket_number",
        validate="m:1",
        how="left",
    )


def scrape_new_filings():
    """Scrape the latest filings."""

    # Initialize the scraper and get the new data
    scraper = NewFilingsScraper()
    new_data = scraper().to_pandas()  # Save scraped columns

    # Format date
    new_data["filing_date"] = pd.to_datetime(new_data["filing_date"])

    # Save latest data
    SORT_COLUMNS = ["filing_date", "docket_number", "defendant_name"]
    new_data.sort_values(SORT_COLUMNS).to_csv(
        DATA_DIR / "raw" / "latest-data.csv", index=False
    )

    # Make combined database
    filename = DATA_DIR / "processed" / "daily-data-historical.csv"

    # Merge together
    if filename.exists():
        data = pd.concat([new_data, pd.read_csv(filename, parse_dates=["filing_date"])])
    else:
        data = new_data.copy()

    # Remove duplicates
    original_length = len(data)
    data = data.drop_duplicates()
    logger.info(f"Removed {original_length - len(data)} duplicate filings")

    # Save
    data = data.sort_values(SORT_COLUMNS)
    data.to_csv(filename, index=False)
