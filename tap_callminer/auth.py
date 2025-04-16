"""CallMiner Authentication."""

from __future__ import annotations

from typing import TYPE_CHECKING

from singer_sdk.authenticators import OAuthAuthenticator, SingletonMeta
from typing_extensions import override

if TYPE_CHECKING:
    from tap_callminer.client import CallMinerStream


# The SingletonMeta metaclass makes your streams reuse the same authenticator instance.
# If this behaviour interferes with your use-case, you can remove the metaclass.
class CallMinerAuthenticator(OAuthAuthenticator, metaclass=SingletonMeta):
    """Authenticator class for CallMiner."""

    @override
    @property
    def oauth_request_body(self):
        return {
            "client_id": self.config["client_id"],
            "client_secret": self.config["client_secret"],
            "grant_type": "client_credentials",
        }

    @classmethod
    def create_for_stream(cls, stream: CallMinerStream) -> CallMinerAuthenticator:
        """Instantiate an authenticator for a CallMiner stream.

        Args:
            stream: The CallMiner stream instance.

        Returns:
            A new authenticator.
        """
        return cls(
            stream=stream,
            auth_endpoint=f"https://idp{stream.region.value}.callminer.net/connect/token",
        )
