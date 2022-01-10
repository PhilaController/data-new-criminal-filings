import pandas as pd

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
