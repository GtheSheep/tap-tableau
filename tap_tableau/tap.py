"""Tableau tap class."""

from typing import List

import tableauserverclient as TSC
from singer_sdk import Tap, Stream
from singer_sdk.helpers._compat import final
from singer_sdk import typing as th  # JSON schema typing helpers
from tap_tableau.streams import (
    DatasourcesStream,
    GroupsStream,
    ProjectsStream,
    SchedulesStream,
    TasksStream,
    WorkbooksStream,
    CustomSQLLocationsMetadataStream,
    PublishedDatasourcesMetadataStream,
    WorkbooksMetadataStream,
)
STREAM_TYPES = [
    DatasourcesStream,
    GroupsStream,
    ProjectsStream,
    SchedulesStream,
    TasksStream,
    WorkbooksStream,
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

    # Not ideal to override this method, but running multiple streams fails if pushed down so watch for timeout issues
    @final
    def sync_all(self) -> None:
        """Sync all streams."""
        self._reset_state_progress_markers()
        self._set_compatible_replication_methods()
        stream: "Stream"
        authentication = TSC.PersonalAccessTokenAuth(self.config['personal_access_token_name'], self.config['personal_access_token_secret'], site_id=self.config.get('site_url_id'))
        server_client = TSC.Server(self.config['server_url'], self.config['api_version']) if self.config.get('api_version') else TSC.Server(self.config['server_url'], use_server_version=True)
        if not server_client.is_signed_in():
            server_client.auth.sign_in(authentication)
        for stream in self.streams.values():
            if not stream.selected and not stream.has_selected_descendents:
                self.logger.info(f"Skipping deselected stream '{stream.name}'.")
                continue
            stream.server_client = server_client
            if stream.parent_stream_type:
                self.logger.debug(
                    f"Child stream '{type(stream).__name__}' is expected to be called "
                    f"by parent stream '{stream.parent_stream_type.__name__}'. "
                    "Skipping direct invocation."
                )
                continue

            stream.sync()
            stream.finalize_state_progress_markers()


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
