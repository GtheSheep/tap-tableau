"""GraphQL client handling, including TableauStream base class and TableauMetadataStream base class."""

import requests
from urllib.parse import urlparse

from singer_sdk.streams import GraphQLStream
from singer_sdk.authenticators import APIKeyAuthenticator
from singer_sdk.streams import RESTStream


class TableauStream(RESTStream):
    """Tableau stream class."""

    url_base = None


class TableauMetadataStream(GraphQLStream):
    """TableauMetadata stream class."""

    api_token = None

    @property
    def authenticator(self) -> APIKeyAuthenticator:
        """Return a new authenticator object."""
        if self.api_token is None:
            self.login()
        return APIKeyAuthenticator.create_for_stream(
            self,
            key="X-tableau-auth",
            value=self.api_token,
            location="header"
        )

    @property
    def url_base(self) -> str:
        """Return the API URL root, configurable via tap settings."""
        return "https://{server}/api/metadata/graphql".format(server=urlparse(self.config['server_url']).hostname)

    def login(self) -> None:
        url = "https://{server}/api/{version}/auth/signin".format(server=urlparse(self.config['server_url']).hostname, version=self.config['api_version'])
        payload = {
            "credentials": {
                "personalAccessTokenName": self.config['personal_access_token_name'],
                "personalAccessTokenSecret": self.config['personal_access_token_secret'],
                "site": {
                    "contentUrl": self.config['site_url_id']
                }
            }
        }
        response = requests.post(url, json=payload, headers=self.http_headers)
        try:
            response.raise_for_status()
            self.logger.info("Login was successful.")
        except Exception as ex:
            raise RuntimeError(
                f"Failed login, response was '{response.json()}'. {ex}"
            )
        response_json = response.json()
        self.api_token = response_json["credentials"]["token"]

    @property
    def http_headers(self) -> dict:
        """Return the http headers needed."""
        headers = {
            'accept': 'application/json',
            'content-type': 'application/json'
        }
        return headers
