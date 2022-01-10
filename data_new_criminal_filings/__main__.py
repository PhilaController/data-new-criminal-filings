import json
from pathlib import Path

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
def update():
    """Scrape the latest data and update the local data files."""
    # Initialize the scraper and get the data
    scraper = NewFilingsScraper()
    data = scraper().to_pandas()
    filing_columns = data.columns.tolist()  # Save scraped columns

    # Format date
    data["filing_date"] = pd.to_datetime(data["filing_date"])

    # Save latest data
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

    # Remove duplicates based on original columns
    original_length = len(data)
    data = data.drop_duplicates(subset=filing_columns)
    logger.info(f"Removed {original_length - len(data)} duplicate filings")

    # Get missing portal results
    missing = data["dc_number"].isnull()
    missing_dockets = data.loc[missing, "docket_number"].drop_duplicates()

    # Get the portal results
    logger.info(f"Scraping info for {len(missing_dockets)} missing dockets")
    portal_results = get_portal_results(missing_dockets)
    logger.info("...done")

    # Combine with past portal results
    portal_columns = portal_results.columns.tolist()
    portal_results = pd.concat(
        [
            data[portal_columns].dropna().drop_duplicates(subset=["docket_number"]),
            portal_results,
        ]
    )
    assert portal_results["docket_number"].duplicated().sum() == 0

    # Merge
    data = data[filing_columns]

    # Merge together
    out = data.merge(
        portal_results,
        on="docket_number",
        validate="m:1",
        how="left",
    )

    # Save
    out = out.sort_values(SORT_COLUMNS)
    out.to_csv(filename, index=False)
