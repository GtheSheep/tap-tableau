"""Tableau tap class."""

from typing import List

from singer_sdk import Tap, Stream
from singer_sdk import typing as th  # JSON schema typing helpers
from tap_tableau.streams import (
    DatasourcesStream,
    CustomSQLLocationsMetadataStream,
    PublishedDatasourcesMetadataStream,
    WorkbooksMetadataStream,
)
STREAM_TYPES = [
    DatasourcesStream,
]
METADATA_STREAM_TYPES = [
    CustomSQLLocationsMetadataStream,
    PublishedDatasourcesMetadataStream,
    WorkbooksMetadataStream,
]


class TapTableau(Tap):
    """Tableau tap class."""
    name = "tap-tableau"

    config_jsonschema = th.PropertiesList(
        th.Property(
            "server_url",
            th.StringType,
            required=True,
            description="Server URL for your Tableau instance"
        ),
        th.Property(
            "api_version",
            th.StringType,
            description="Version of REST API to use"
        ),
        th.Property(
            "site_url_id",
            th.StringType,
            description="Site ID to query"
        ),
        th.Property(
            "personal_access_token_name",
            th.StringType,
            required=True,
            description="Name for the personal access token for authentication"
        ),
        th.Property(
            "personal_access_token_secret",
            th.StringType,
            required=True,
            description="Personal access token for authentication"
        ),
    ).to_dict()

    def discover_streams(self) -> List[Stream]:
        """Return a list of discovered streams."""
        return [stream_class(tap=self) for stream_class in STREAM_TYPES]


class TapTableauMetadata(Tap):
    """TableauMetadata tap class."""
    name = "tap-tableau-metadata"

    config_jsonschema = th.PropertiesList(
        th.Property(
            "server_url",
            th.StringType,
            required=True,
            description="Server URL for your Tableau instance"
        ),
        th.Property(
            "api_version",
            th.StringType,
            description="Version of REST API to use"
        ),
        th.Property(
            "site_url_id",
            th.StringType,
            description="Site ID to query"
        ),
        th.Property(
            "personal_access_token_name",
            th.StringType,
            required=True,
            description="Name for the personal access token for authentication"
        ),
        th.Property(
            "personal_access_token_secret",
            th.StringType,
            required=True,
            description="Personal access token for authentication"
        ),
    ).to_dict()

    def discover_streams(self) -> List[Stream]:
        """Return a list of discovered streams."""
        return [stream_class(tap=self) for stream_class in METADATA_STREAM_TYPES]
