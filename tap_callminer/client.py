"""REST client handling, including CallMinerStream base class."""

from __future__ import annotations

from functools import cached_property

from singer_sdk.helpers._typing import TypeConformanceLevel
from singer_sdk.streams import RESTStream
from typing_extensions import override

from tap_callminer import CallMinerAPIRegion
from tap_callminer.auth import CallMinerAuthenticator


class CallMinerStream(RESTStream):
    """CallMiner stream class."""

    TYPE_CONFORMANCE_LEVEL = TypeConformanceLevel.NONE

    @cached_property
    def region(self):
        """Parse API region enum value from config."""
        return CallMinerAPIRegion[self.config["region"]]

    @override
    @cached_property
    def url_base(self):
        return f"https://api{self.region.value}.callminer.net/bulkexport/api"

    @override
    @cached_property
    def authenticator(self):
        return CallMinerAuthenticator.create_for_stream(self)
