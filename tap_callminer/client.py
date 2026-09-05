# Copyright (c) 2026 Meltano.

"""REST client handling, including CallMinerStream base class."""

from __future__ import annotations

from functools import cached_property

from requests.adapters import HTTPAdapter, Retry
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
    def requests_session(self):
        # An export job leaves this pool idle for as long as the download and
        # downstream processing take - hours, for a large export - by which point
        # intermediaries have usually dropped the socket without sending a FIN, so
        # urllib3's own liveness check can't spot it. Retrying at the transport layer
        # gets a fresh connection. Only idempotent methods are retried by default, so
        # the job-creating POST is never replayed. Status-based retries are left to
        # `validate_response` and the SDK's backoff handling.
        session = super().requests_session
        adapter = HTTPAdapter(max_retries=Retry(total=3, backoff_factor=1))
        session.mount("https://", adapter)
        session.mount("http://", adapter)
        return session

    @cached_property
    def _send_with_retries(self):
        # `request_decorator` is what supplies retry and backoff; the SDK only applies
        # it to `_request` from within `request_records`, so requests made outside the
        # record flow get no backoff unless they wrap themselves.
        #
        # Deliberately not reusing `_request`: it logs metrics against `self.path`,
        # which the data type streams have no reason to define, and which would
        # mislabel these calls on the streams that do. Re-applying the authenticator
        # per attempt is what lets a retry recover from a token that expired while a
        # long export was in flight.
        def send(prepared_request):
            response = self.requests_session.send(
                self.authenticator(prepared_request),
                timeout=self.timeout,
                allow_redirects=self.allow_redirects,
            )
            self.validate_response(response)
            return response

        return self.request_decorator(send)

    def send_request(self, method, url, **kwargs):
        """Make a request outside the paginated record flow.

        Applies the same retry, backoff and response validation the SDK gives to
        record requests.

        Args:
            method: HTTP method.
            url: Fully qualified URL.
            kwargs: Additional arguments for :class:`requests.Request`.

        Returns:
            The validated :class:`requests.Response`.
        """
        return self._send_with_retries(
            self.build_prepared_request(method=method, url=url, **kwargs),
        )

    @override
    @cached_property
    def url_base(self):
        return f"https://api{self.region.value}.callminer.net/bulkexport/api"

    @override
    @cached_property
    def authenticator(self):
        return CallMinerAuthenticator.create_for_stream(self)
