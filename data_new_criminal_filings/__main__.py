import json

import click
import pandas as pd
from loguru import logger
from phl_courts_scraper.new_filings import NewFilingsScraper
from phl_courts_scraper_batch.aws import APP_NAME, AWS

from . import DATA_DIR


@click.group()
@click.version_option()
def cli():
    """Processing data for the New Criminal Filings."""
    pass


def get_portal_results(dockets, ntasks=20, sleep=2):
    """Get the portal results."""

    # Initialize AWS
    aws = AWS()
    bucket = APP_NAME
    dataset = "new-criminal-filings"
    path = f"{bucket}/datasets/{dataset}.csv"

    # Save to AWS
    with aws.remote.open(path, "w") as ff:
        dockets.to_csv(ff, index=False, header=False)

    # Run the jobs
    result_file = aws.submit_jobs(
        flavor="portal",
        dataset="new-criminal-filings",
        search_by="Docket Number",
        sleep=sleep,
        ntasks=ntasks,
        wait=True,
    )

    # Load the result
    with aws.remote.open(
        result_file,
        "r",
    ) as ff:
        data = json.loads(ff.read())

    # Get the portal results
    portal_results = (
        pd.DataFrame([dd for l in data for dd in l if len(l)])
        .drop_duplicates()
        .drop(labels=["filing_date"], axis=1)
    )

    return portal_results


@cli.command()
@click.option("--ntasks", type=int, default=2)
def update(ntasks=2):
    """Scrape the latest data and update the local data files."""
    # Initialize the scraper and get the data
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

    # Load existing portal results
    filename = DATA_DIR / "processed" / "portal-results-historical.csv"
    existing_portal_results = pd.read_csv(filename)

    # Get data without any portal results
    missing = ~data["docket_number"].isin(existing_portal_results["docket_number"])
    missing_dockets = data.loc[missing, "docket_number"].drop_duplicates()

    # Get the portal results
    logger.info(f"Scraping info for {len(missing_dockets)} missing dockets")
    new_portal_results = get_portal_results(missing_dockets, ntasks=ntasks)
    logger.info("...done")

    # Combine with past portal results
    portal_results = pd.concat(
        [
            existing_portal_results,
            new_portal_results,
        ]
    )
    assert portal_results["docket_number"].duplicated().sum() == 0

    # Save portal results
    portal_results.to_csv(filename, index=False)
