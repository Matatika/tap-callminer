"""CallMiner tap class."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone

from singer_sdk import Tap
from singer_sdk import typing as th  # JSON schema typing helpers
from singer_sdk.contrib.msgspec import MsgSpecWriter
from typing_extensions import override

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
            "notification_email",
            th.EmailType,
            title="Notification email address",
            description=(
                "Email address required by CallMiner to send a notification to once an "
                "export completes"
            ),
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
            th.DateType,
            default=(
                (datetime.now(tz=timezone.utc) - timedelta(days=365)).date().isoformat()
            ),
            description="The earliest record date to sync",
        ),
        th.Property(
            "job_poll_max_count",
            th.IntegerType(minimum=1),
            default=60,
            description="Maximum job poll count",
        ),
    ).to_dict()

    message_writer_class = MsgSpecWriter

    @override
    def discover_streams(self) -> list[streams.CallMinerStream]:
        data_type_streams: list[streams.DataTypeStream] = [
            streams.AISummariesStream(self),
            streams.AlertsStream(self),
            streams.CategoriesStream(self),
            streams.CategoryComponentsStream(self),
            streams.ClientIDsStream(self),
            streams.CoachInsightsStream(self),
            streams.CoachWorkflowsStream(self),
            streams.CommentsStream(self),
            streams.ContactsStream(self),
            streams.EmailMetadataStream(self),
            streams.EventsDelayStream(self),
            streams.EventsMissingRealTimeSegementsStream(self),
            streams.EventsOvertalkStream(self),
            streams.EventsRedactionStream(self),
            streams.EventsSilenceStream(self),
            streams.ScoresStream(self),
            streams.ScoreIndicatorsStream(self),
            streams.TagsStream(self),
            streams.TranscriptsBySpeakerStream(self),
        ]

        export_stream = streams.ExportStream(self)
        export_stream.data_types = list(
            dict.fromkeys(
                s.data_type
                for s in data_type_streams
                if not self.catalog
                or self.catalog.get_stream(s.tap_stream_id).metadata.root.selected
            )
        )

        return [
            export_stream,
            *data_type_streams,
        ]


if __name__ == "__main__":
    TapCallMiner.cli()
