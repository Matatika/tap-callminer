"""CallMiner tap class."""

from __future__ import annotations

from singer_sdk import Tap
from singer_sdk import typing as th  # JSON schema typing helpers

# TODO: Import your custom stream types here:
from tap_callminer import CallMinerAPIRegion, streams


class TapCallMiner(Tap):
    """CallMiner tap class."""

    name = "tap-callminer"

    config_jsonschema = th.PropertiesList(
        th.Property(
            "client_id",
            th.StringType,
            required=True,
            title="Client ID",
            description="CallMiner bulk export API client ID",
        ),
        th.Property(
            "client_secret",
            th.StringType,
            required=True,
            secret=True,
            title="Client secret",
            description="CallMiner bulk export API client secret",
        ),
        th.Property(
            "region",
            th.StringType,
            title="Region",
            allowed_values=[r.name for r in CallMinerAPIRegion],
            default=CallMinerAPIRegion.US.name,
            description="CallMiner API region",
        ),
        th.Property(
            "start_date",
            th.DateTimeType,
            default=(datetime.now(tz=timezone.utc) - timedelta(days=365)).isoformat(),
            description="The earliest record date to sync",
        ),
    ).to_dict()

    def discover_streams(self) -> list[streams.CallMinerStream]:
        """Return a list of discovered streams.

        Returns:
            A list of discovered streams.
        """
        return [
            streams.GroupsStream(self),
            streams.UsersStream(self),
        ]


if __name__ == "__main__":
    TapCallMiner.cli()
