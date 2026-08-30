"""GHArchiveSensor — custom sensor for GitHub Archive availability.

The sensor waits until the hourly GitHub Archive file for the DAG's
logical date becomes available.
"""

from __future__ import annotations

import requests

from airflow.sensors.base import BaseSensorOperator


class GHArchiveSensor(BaseSensorOperator):
    def __init__(self, hour: int = 14, **kwargs) -> None:
        super().__init__(**kwargs)
        self.hour = hour

    def poke(self, context) -> bool:
        # Get the DAG's logical date from the Airflow context.
        ds = context["ds"]

        # Build the GitHub Archive URL for the requested date and hour.
        url = f"https://data.gharchive.org/{ds}-{self.hour:02d}.json.gz"

        try:
            # Use HEAD because we only need to check whether the file exists.
            response = requests.head(url, timeout=10)

            # Continue only when the archive file is available.
            return response.status_code == 200

        except requests.RequestException:
            # Keep waiting if the request fails.
            return False

        