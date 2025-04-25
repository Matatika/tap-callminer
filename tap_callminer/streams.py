"""Stream type classes for tap-callminer."""

from __future__ import annotations

import abc
import csv
import decimal
import gzip
import itertools
import math
import tempfile
import time
import zipfile
from datetime import datetime, timezone
from functools import cached_property
from pathlib import Path

from singer_sdk import typing as th
from typing_extensions import override

from tap_callminer.client import CallMinerStream

UINT8_TYPE = th.IntegerType(minimum=-(2**7), maximum=2**7 - 1)
UINT16_TYPE = th.IntegerType(minimum=-(2**15), maximum=2**15 - 1)
UINT32_TYPE = th.IntegerType(minimum=-(2**31), maximum=2**31 - 1)


DOTNET_JSON_SCHEMA_TYPES = {
    "System.String": th.StringType,
    "System.Byte": UINT8_TYPE,
    "System.Int16": UINT16_TYPE,
    "System.Int32": UINT32_TYPE,
    "System.Single": th.NumberType,
    "System.Double": th.NumberType,
    "System.Decimal": th.NumberType,
    "System.DateTime": th.DateTimeType,
    "System.TimeSpan": th.TimeType,
    "System.Guid": th.UUIDType,
    "System.Boolean": th.BooleanType,
}


DEFAULT_JSON_SCHEMA_TYPE = th.StringType


class ExportStream(CallMinerStream):
    """Define export stream."""

    data_types: tuple[str] = []

    name = "__export__"
    schema = th.ObjectType().to_dict()
    http_method = "POST"
    path = "/export/job"

    @override
    def prepare_request_payload(self, context, next_page_token):
        return {
            "name": self.tap_name,
            "DataTypes": self.data_types,
            "Duration": {
                "TimeFrame": "Custom",
                "StartDate": (
                    self.stream_state.get("start_date") or self.config["start_date"]
                ),
                "SearchMode": "NewAndUpdated",
            },
            "EmailRecipients": [self.config["notification_email"]],
        }

    @override
    def parse_response(self, response):
        job_poll_max_count: int = self.config["job_poll_max_count"]

        job = response.json()

        job_id = job["Id"]
        job_execution_id = None

        try:
            for count in itertools.count(1):
                if count > job_poll_max_count:
                    msg = (
                        f"Export job incomplete (polled {job_poll_max_count} time(s) "
                        "at 60s intervals. `job_poll_max_count` may need adjusting."
                    )
                    raise RuntimeError(msg)

                response = self.requests_session.send(
                    self.build_prepared_request(
                        method="GET",
                        url=f"{self.url_base}/export/history",
                        params={"id": job_id},
                    ),
                    timeout=self.timeout,
                    allow_redirects=self.allow_redirects,
                )

                job_execution = next(iter(response.json()), None)

                if not job_execution:
                    self.logger.info("Job execution not yet started")
                else:
                    if not job_execution_id:
                        job_execution_id = job_execution["Id"]
                        self.logger.info(
                            "Job execution %s started at %s",
                            job_execution_id,
                            job_execution["CreateDate"],
                        )

                    job_execution_status = job_execution["Status"]

                    self.logger.info(
                        "Job execution %s status: %s",
                        job_execution_id,
                        job_execution_status,
                    )

                    if job_execution_status == "Completed":
                        job_execution_completed_at = job_execution["JobCompletionTime"]

                        self.logger.info(
                            "Job execution %s completed at %s",
                            job_execution_id,
                            job_execution_completed_at,
                        )

                        break

                self.logger.info("Waiting 60s... (%d/%d)", count, job_poll_max_count)
                time.sleep(60)  # poll every minute, following CallMiner best practices

            file_size_mb = job_execution["FileSize"] / 1000**2  # convert to MB

            with (
                self.requests_session.send(
                    self.build_prepared_request(
                        method="GET",
                        url=job_execution["DownloadEndpoint"],
                    ),
                    stream=True,
                    timeout=self.timeout,
                    allow_redirects=self.allow_redirects,
                ) as response,
                tempfile.TemporaryDirectory(prefix=f"{self.tap_name}-") as tmpdir,
            ):
                response.raise_for_status()

                zip_path = Path(tmpdir) / f"{job_id}-{job_execution_id}.zip"
                self.logger.info(
                    "Downloading file: %s (%.1f MB)",
                    zip_path,
                    file_size_mb,
                )

                with zip_path.open("wb") as f:
                    for chunk in response.iter_content(1024**2):  # 1 MB
                        f.write(chunk)

                with zipfile.ZipFile(zip_path) as z:
                    z.extractall(tmpdir)

                zip_path.unlink()

                yield {"tmp_dir": tmpdir, "job_execution_id": job_execution_id}

                self.stream_state["start_date"] = (
                    datetime.now(tz=timezone.utc).date().isoformat()
                )

        finally:
            self.logger.info("Cleaning up job %s", job_id)

            response = self.requests_session.send(
                self.build_prepared_request(
                    method="DELETE",
                    url=f"{self.url_base}/export/job/{job_id}",
                ),
                timeout=self.timeout,
                allow_redirects=self.allow_redirects,
            )
            response.raise_for_status()

            if job_execution_id:
                self.logger.info("Cleaning up job execution %s", job_execution_id)

                response = self.requests_session.send(
                    self.build_prepared_request(
                        method="DELETE",
                        url=f"{self.url_base}/export/history/{job_execution_id}",
                    ),
                    timeout=self.timeout,
                    allow_redirects=self.allow_redirects,
                )
                response.raise_for_status()


class DataTypeStream(CallMinerStream):
    """Define data type stream."""

    parent_stream_type = ExportStream
    state_partitioning_keys = ()

    @property
    @abc.abstractmethod
    def data_type(self) -> str:
        """CallMiner export job data type."""

    @property
    def filename_data_type(self):
        """Data type as given in export job file names."""
        return self.data_type.title()

    @override
    def get_records(self, context):
        tmp_dir: str = context["tmp_dir"]
        job_execution_id: str = context["job_execution_id"]
        filepath = (
            Path(tmp_dir) / f"{job_execution_id}_{self.filename_data_type}.csv.gz"
        )

        with gzip.open(filepath, "r") as f:
            for record in csv.DictReader(line.decode("utf-8-sig") for line in f):
                yield self.post_process(record)

    @override
    def post_process(self, row, context=None):
        row = super().post_process(row, context=context)

        properties: dict = self.schema["properties"]

        for k in row:
            value: str = row[k]

            if value is None or k not in properties:
                continue

            property_type = properties[k]["type"]

            if "integer" in property_type or "number" in property_type:
                try:
                    d = decimal.Decimal(value)
                except decimal.DecimalException:
                    d = decimal.Decimal(math.nan)
                    self.logger.debug("Handling invalid decimal '%s' as %s", value, d)

                value = d

                if d.is_nan() or d.is_infinite():
                    value = None
                    self.logger.debug(
                        (
                            "%s is not supported as a numeric value in JSON, handling "
                            "as %s"
                        ),
                        d,
                        value,
                    )
            elif "boolean" in property_type:
                value = value.lower() == "true"

            row[k] = value

        return row

    def _dotnet_to_json_schema_type(
        self,
        dotnet_type: str,
    ) -> th.JSONTypeHelper:
        json_schema_type = DOTNET_JSON_SCHEMA_TYPES.get(dotnet_type)

        if json_schema_type:
            return json_schema_type

        self.logger.warning(
            (
                "No JSON schema type mapping defined for DotNet type '%s', defaulting"
                "to %s"
            ),
            dotnet_type,
            DEFAULT_JSON_SCHEMA_TYPE,
        )

        return DEFAULT_JSON_SCHEMA_TYPE


class AISummariesStream(DataTypeStream):
    """Define AI summaries stream."""

    name = "ai_summaries"
    schema = th.PropertiesList(
        th.Property("ContactID", UINT32_TYPE),
        th.Property("Summary", th.StringType),
        th.Property("ActionItems", th.StringType),
        th.Property("Reason", th.StringType),
    ).to_dict()

    primary_keys = ("ContactID",)
    data_type = "Ai_summaries"


class AlertsStream(DataTypeStream):
    """Define alerts stream."""

    name = "alerts"
    schema = th.PropertiesList(
        th.Property("ContactID", UINT32_TYPE),
        th.Property("AlertID", UINT32_TYPE),
        th.Property("AlertName", th.StringType),
        th.Property("TimeStamp", th.DateTimeType),
    ).to_dict()

    primary_keys = ("ContactID", "TimeStamp")
    data_type = "Alerts"


class CategoriesStream(DataTypeStream):
    """Define categories stream."""

    name = "categories"
    schema = th.PropertiesList(
        th.Property("ContactID", UINT32_TYPE),
        th.Property("CategoryID", UINT32_TYPE),
        th.Property("CategoryFullName", th.StringType),
        th.Property("CategoryName", th.StringType),
        th.Property("SectionID", UINT32_TYPE),
        th.Property("SectionName", th.StringType),
    ).to_dict()

    primary_keys = ("ContactID", "CategoryID")
    data_type = "Categories"


class CategoryComponentsStream(DataTypeStream):
    """Define category components stream."""

    name = "category_components"
    schema = th.PropertiesList(
        th.Property("ContactID", UINT32_TYPE),
        th.Property("ComponentID", UINT32_TYPE),
        th.Property("ComponentName", th.StringType),
        th.Property("CategoryID", UINT32_TYPE),
        th.Property("CategoryFullName", th.StringType),
        th.Property("CategoryDescription", th.StringType),
        th.Property("CategoryName", th.StringType),
        th.Property("SectionName", th.StringType),
        th.Property("StartTime", th.NumberType),
        th.Property("EndTime", th.NumberType),
        th.Property("Weight", th.NumberType),
    ).to_dict()

    primary_keys = ("ContactID", "ComponentID", "CategoryID")
    data_type = "Category_components"


class ClientIDsStream(DataTypeStream):
    """Define client IDs stream."""

    name = "client_ids"
    schema = th.PropertiesList(
        th.Property("ContactID", UINT32_TYPE),
        th.Property("ClientID", th.StringType),
        th.Property("ClientCaptureDate", th.DateTimeType),
    ).to_dict()

    primary_keys = ("ContactID", "ClientID")
    data_type = "Client_Ids"


class CoachInsightsStream(DataTypeStream):
    """Define coach insights stream."""

    name = "coach_insights"
    schema = th.PropertiesList(
        th.Property("ContactID", UINT32_TYPE),
        th.Property("InsightID", th.StringType),
        th.Property("AssignedByUser", th.EmailType),
        th.Property("AssignedBy", th.StringType),
        th.Property("CreationDate", th.DateTimeType),
        th.Property("AssignedToEmail", th.EmailType),
        th.Property("AssignedTo", th.StringType),
        th.Property("OriginalDueDate", th.DateTimeType),
        th.Property("DueDate", th.DateTimeType),
        th.Property("CompletedDate", th.DateTimeType),
        th.Property("Indicators", th.StringType),
        th.Property("Status", th.StringType),
        th.Property("CurrentOwner", th.StringType),
        th.Property("LastUpdated", th.DateTimeType),
        th.Property("Acknowledged", th.BooleanType),
        th.Property("FirstListenedTime", th.DateTimeType),
        th.Property("ContactType", th.StringType),
        th.Property("ContactDate", th.DateTimeType),
        th.Property("AgentEmail", th.EmailType),
        th.Property("DisplayDescriptionsInCoach", th.BooleanType),
        th.Property("SnippetStart", UINT32_TYPE),
        th.Property("SnippetEnd", UINT32_TYPE),
        th.Property("ContactLength", UINT32_TYPE),
        th.Property("ScoreID", UINT16_TYPE),
        th.Property("ScoreName", th.StringType),
        th.Property("OriginalScoreValue", th.NumberType),
        th.Property("ScoreValue", th.NumberType),
        th.Property("CommentDate", th.DateTimeType),
        th.Property("CommentCreatorEmail", th.EmailType),
        th.Property("CommentCreatorName", th.StringType),
        th.Property("Comments", th.StringType),
        th.Property("Type", th.StringType),
        th.Property("CompletedByEmail", th.EmailType),
        th.Property("CompletedBy", th.StringType),
        th.Property("Forwarded", th.StringType),
        th.Property("OriginalInsightID", th.StringType),
    ).to_dict()

    primary_keys = ("ContactID", "InsightID")
    data_type = "Coach_insights"


class CoachWorkflowsStream(DataTypeStream):
    """Define coach workflows stream."""

    name = "coach_workflows"
    schema = th.PropertiesList(
        th.Property("ContactID", UINT32_TYPE),
        th.Property("ID", th.StringType),
        th.Property("NewOwner", th.StringType),
        th.Property("NewStatus", th.StringType),
        th.Property("OldOwner", th.StringType),
        th.Property("OldStatus", th.StringType),
        th.Property("Timestamp", th.DateTimeType),
        th.Property("UserName", th.StringType),
        th.Property("ChangeType", th.StringType),
    ).to_dict()

    primary_keys = ("ContactID", "ID", "Timestamp")
    data_type = "Coach_workflow"
    filename_data_type = "Coach_Workflows"


class CommentsStream(DataTypeStream):
    """Define comments stream."""

    name = "comments"
    schema = th.PropertiesList(
        th.Property("ContactID", UINT32_TYPE),
        th.Property("CommentID", UINT32_TYPE),
        th.Property("Comment", th.StringType),
        th.Property("CreationDate", th.DateTimeType),
        th.Property("Author", th.StringType),
        th.Property("LastUser", th.StringType),
        th.Property("LastModified", th.DateTimeType),
    ).to_dict()

    primary_keys = ("ContactID", "CommentID")
    data_type = "Comments"


class ContactsStream(DataTypeStream):
    """Define contacts stream."""

    name = "contacts"
    primary_keys = ("ContactID",)
    data_type = "Contacts"

    @override
    @cached_property
    def schema(self):
        response = self.requests_session.send(
            self.build_prepared_request(
                method="GET",
                url=f"{self.url_base}/info/callmetadataconfig",
            ),
            timeout=self.timeout,
            allow_redirects=self.allow_redirects,
        )
        response.raise_for_status()

        call_metadata_config: list[dict[str]] = response.json()

        schema = th.PropertiesList(
            *(
                th.Property(
                    metadata["ColumnName"],
                    self._dotnet_to_json_schema_type(metadata["DotNetType"]),
                    title=metadata["FriendlyName"],
                    description=metadata["Description"],
                )
                for metadata in call_metadata_config
            )
        )

        return schema.to_dict()

    def _dotnet_to_json_schema_type(
        self,
        dotnet_type: str,
    ) -> th.JSONTypeHelper:
        json_schema_type = DOTNET_JSON_SCHEMA_TYPES.get(dotnet_type)

        if json_schema_type:
            return json_schema_type

        self.logger.warning(
            (
                "No JSON schema type mapping defined for DotNet type '%s', defaulting"
                "to %s"
            ),
            dotnet_type,
            DEFAULT_JSON_SCHEMA_TYPE,
        )

        return DEFAULT_JSON_SCHEMA_TYPE


class EmailMetadataStream(DataTypeStream):
    """Define email metadata stream."""

    name = "email_metadata"
    schema = th.PropertiesList(
        th.Property("ContactID", UINT32_TYPE),
        th.Property("StartTime", th.NumberType),
        th.Property("Sent", th.NumberType),
        th.Property("Subject", th.StringType),
        th.Property("From", th.StringType),
        th.Property("To", th.StringType),
        th.Property("CC", th.StringType),
        th.Property("BCC", th.StringType),
        th.Property("Attachments", th.StringType),
    ).to_dict()

    primary_keys = ("ContactID", "StartTime")
    data_type = "Email_metadata"

class _EventsStream(DataTypeStream):
    primary_keys = ("ContactID", "StartTime", "EndTime")
    data_type = "Events"
    additional_properties = ()

    @override
    @cached_property
    def schema(self):
        return th.PropertiesList(
            th.Property("ContactID", UINT32_TYPE),
            th.Property("StartTime", th.NumberType),
            th.Property("EndTime", th.NumberType),
            th.Property("Duration", th.NumberType),
            *self.additional_properties,
        ).to_dict()


class EventsDelayStream(_EventsStream):
    """Define delay events stream."""

    name = "events_delay"
    filename_data_type = "Events_Delay"
    additional_properties = (
        th.Property("Speaker", UINT8_TYPE),
        th.Property("PreviousSpeaker", UINT8_TYPE),
    )


class EventsMissingRealTimeSegementsStream(_EventsStream):
    """Define missing real-time segments events stream."""

    name = "events_missing_real_time_segments"
    filename_data_type = "Events_MissingRealTimeSegements"


class EventsOvertalkStream(_EventsStream):
    """Define overtalk events stream."""

    name = "events_overtalk"
    filename_data_type = "Events_Overtalk"


class EventsRedactionStream(_EventsStream):
    """Define redaction events stream."""

    name = "events_redaction"
    filename_data_type = "Events_Redaction"
    additional_properties = (
        th.Property("FriendlyName", th.StringType),
        th.Property("Text", th.StringType),
        th.Property("EntityType", th.StringType),
        th.Property("Description", th.StringType),
        th.Property("RegulatoryCompliance", th.StringType),
        th.Property("SpeakerID", UINT8_TYPE),
        th.Property("SpeakerName", th.StringType),
    )


class EventsSilenceStream(_EventsStream):
    """Define silence events stream."""

    name = "events_silence"
    filename_data_type = "Events_Silence"


class ScoresStream(DataTypeStream):
    """Define scores stream."""

    name = "scores"
    schema = th.PropertiesList(
        th.Property("ContactID", UINT32_TYPE),
        th.Property("ScoreID", UINT16_TYPE),
        th.Property("ScoreName", th.StringType),
        th.Property("Score", th.NumberType),
    ).to_dict()

    primary_keys = ("ContactID", "ScoreID")
    data_type = "Scores"


class ScoreIndicatorsStream(DataTypeStream):
    """Define score indicators stream."""

    name = "score_indicators"
    schema = th.PropertiesList(
        th.Property("ContactID", UINT32_TYPE),
        th.Property("ScoreID", UINT16_TYPE),
        th.Property("ScoreComponentID", UINT32_TYPE),
        th.Property("ComponentFullName", th.StringType),
        th.Property("DisplayDescription", th.BooleanType),
        th.Property("CurrentlyActive", th.BooleanType),
        th.Property("DisplayFormat", UINT8_TYPE),
        th.Property("DisplayOrder", UINT16_TYPE),
        th.Property("Value", th.NumberType),
    ).to_dict()

    primary_keys = ("ContactID", "ScoreID", "ScoreComponentID")
    data_type = "Score_indicators"


class TagsStream(DataTypeStream):
    """Define tags stream."""

    name = "tags"
    schema = th.PropertiesList(
        th.Property("ContactID", UINT32_TYPE),
        th.Property("TagID", UINT32_TYPE),
        th.Property("TagName", th.StringType),
        th.Property("TagFullName", th.StringType),
    ).to_dict()

    primary_keys = ("ContactID", "TagID")
    data_type = "Tags"


class TranscriptsBySpeakerStream(DataTypeStream):
    """Define transcripts by speaker stream."""

    name = "transcripts_by_speaker"
    schema = th.PropertiesList(
        th.Property("ContactID", UINT32_TYPE),
        th.Property("StartTime", th.NumberType),
        th.Property("SpeakerID", UINT8_TYPE),
        th.Property("SpeakerName", th.StringType),
        th.Property("Text", th.StringType),
    ).to_dict()

    primary_keys = ("ContactID", "StartTime", "SpeakerID")
    data_type = "Transcripts_by_speaker"
