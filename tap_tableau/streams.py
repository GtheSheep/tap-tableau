"""Stream type classes for tap-tableau-metadata."""

from pathlib import Path
from typing import Any, Dict, Optional, Union, List, Iterable

import requests
import tableauserverclient as TSC
from singer_sdk import typing as th  # JSON Schema typing helpers

from tap_tableau.client import TableauMetadataStream
from tap_tableau.client import TableauStream
from tap_tableau.utils import format_datetime
from tap_tableau.utils import get_permission_details


class DatasourcesStream(TableauStream):
    name = "datasources"
    primary_keys = ["id"]
    replication_key = None
    schema = th.PropertiesList(
        th.Property("id", th.StringType),
        th.Property("ask_data_enablement", th.BooleanType),
        th.Property("certification_note", th.StringType),
        th.Property("certified", th.BooleanType),
        th.Property("connections", th.ArrayType(
            th.ObjectType(
                th.Property("connection_type", th.StringType),
                th.Property("datasource_id", th.StringType),
                th.Property("datasource_name", th.StringType),
                th.Property("embed_password", th.BooleanType),
                th.Property("id", th.StringType),
                th.Property("server_address", th.StringType),
                th.Property("server_port", th.NumberType),
                th.Property("username", th.StringType),
           )
        )),
        th.Property("content_url", th.StringType),
        th.Property("created_at", th.DateTimeType),
        th.Property("datasource_type", th.StringType),
        th.Property("description", th.StringType),
        th.Property("encrypt_extracts", th.BooleanType),
        th.Property("has_extracts", th.BooleanType),
        th.Property("name", th.StringType),
        th.Property("owner_id", th.StringType),
        th.Property("permissions", th.ArrayType(
            th.ObjectType(
                th.Property("capabilities", th.ObjectType(
                    th.Property("Connect", th.StringType),
                    th.Property("Read", th.StringType),
                    th.Property("Write", th.StringType),
                )),
                th.Property("grantee_id", th.StringType),
                th.Property("grantee_tag_name", th.StringType),
            )
        )),
        th.Property("project_id", th.StringType),
        th.Property("project_name", th.StringType),
        th.Property("tags", th.ArrayType(th.StringType)),
        th.Property("updated_at", th.DateTimeType),
        th.Property("use_remote_query_agent", th.BooleanType),
    ).to_dict()

    def get_records(self, context: Optional[dict]) -> Iterable[dict]:
        """Return a generator of row-type dictionary objects.
        """
        for datasource in TSC.Pager(self.server_client.datasources):
            self.server_client.datasources.populate_connections(datasource)
            self.server_client.datasources.populate_permissions(datasource)
            row = {
                'ask_data_enablement': datasource.ask_data_enablement,
                'certification_note': datasource.certification_note,
                'certified': datasource.certified,
                'connections': [{
                    'connection_type': connection.connection_type,
                    'datasource_id': connection.datasource_id,
                    'datasource_name': connection.datasource_name,
                    'embed_password': connection.embed_password,
                    'id': connection.id,
                    'server_address': connection.server_address,
                    'server_port': connection.server_port,
                    'username': connection.username
                } for connection in datasource.connections],
                'content_url': datasource.content_url,
                'created_at': format_datetime(datasource.created_at),
                'datasource_type': datasource.datasource_type,
                'description': datasource.description,
                'encrypt_extracts': datasource.encrypt_extracts,
                'has_extracts': datasource.has_extracts,
                'id': datasource.id,
                'name': datasource.name,
                'owner_id': datasource.owner_id,
                'permissions': [get_permission_details(permission) for permission in datasource.permissions],
                'project_id': datasource.project_id,
                'project_name': datasource.project_name,
                'tags': list(datasource.tags),
                'updated_at': format_datetime(datasource.updated_at),
                'use_remote_query_agent': datasource.use_remote_query_agent,
            }
            yield row


class WorkbooksMetadataStream(TableauMetadataStream):
    name = "workbooks_metadata"
    schema = th.PropertiesList(
        th.Property("id", th.StringType),
        th.Property("luid", th.StringType),
        th.Property("name", th.StringType),
        th.Property("description", th.StringType),
        th.Property("createdAt", th.DateTimeType),
        th.Property("siteLuid", th.StringType),
        th.Property("projectName", th.StringType),
        th.Property("projectVizportalUrlId", th.StringType),
        th.Property("ownerId", th.StringType),
        th.Property("uri", th.StringType),
        th.Property("upstreamDatasources", th.ArrayType(
            th.ObjectType(
                th.Property("id", th.StringType),
                th.Property("luid", th.StringType),
                th.Property("name", th.StringType)
            )
        )),
        th.Property("embeddedDatasources", th.ArrayType(
            th.ObjectType(
                th.Property("id", th.StringType),
                th.Property("name", th.StringType)
            )
        )),
    ).to_dict()
    primary_keys = ["id"]
    replication_key = None

    @property
    def query(self) -> str:
        return """
            query workbooks{
                workbooks {
                    id
                    luid
                    name
                    description
                    createdAt
                    site {
                        luid
                    }
                    projectName
                    projectVizportalUrlId
                    owner {
                        id
                    }
                    uri
                    upstreamDatasources {
                        id
                        luid
                        name
                    }
                    embeddedDatasources {
                        id
                        name
                    }
                    }
                }
        """

    def parse_response(self, response: requests.Response) -> Iterable[dict]:
        resp_json = response.json()
        for row in resp_json["data"]["workbooks"]:
            row["siteLuid"] = row["site"]["luid"]
            row["ownerId"] = row["owner"]["id"]
            yield row


class PublishedDatasourcesMetadataStream(TableauMetadataStream):
    name = "published_datasources_metadata"
    schema = th.PropertiesList(
        th.Property("id", th.StringType),
        th.Property("luid", th.StringType),
        th.Property("name", th.StringType),
        th.Property("hasUserReference", th.BooleanType),
        th.Property("hasExtracts", th.BooleanType),
        th.Property("siteLuid", th.StringType),
        th.Property("projectName", th.StringType),
        th.Property("projectVizportalUrlId", th.StringType),
        th.Property("ownerId", th.StringType),
        th.Property("isCertified", th.BooleanType),
        th.Property("certifierLuid", th.StringType),
        th.Property("certificationNote", th.StringType),
        th.Property("certifierDisplayName", th.StringType),
        th.Property("description", th.StringType),
        th.Property("downstreamWorkbooks", th.ArrayType(
            th.ObjectType(
                th.Property("id", th.StringType),
                th.Property("luid", th.StringType),
                th.Property("name", th.StringType)
            )
        )),
    ).to_dict()
    primary_keys = ["id"]
    replication_key = None

    @property
    def query(self) -> str:
        return """
            query published_datasources{
              publishedDatasources {
                id
                luid
                name
                hasUserReference
                hasExtracts
                extractLastRefreshTime
                site{
                    luid
                }
                projectName
                projectVizportalUrlId
                owner{
                  luid
                }
                isCertified
                certifier {
                  luid
                }
                certificationNote
                certifierDisplayName
                description
                downstreamWorkbooks {
                    id
                    luid
                    name
                }
              }
            }
        """

    def parse_response(self, response: requests.Response) -> Iterable[dict]:
        resp_json = response.json()
        for row in resp_json["data"]["publishedDatasources"]:
            row["siteLuid"] = row["site"]["luid"]
            row["ownerLuid"] = row["owner"]["luid"]
            row["certifierLuid"] = row["owner"]["luid"]
            yield row


class CustomSQLLocationsMetadataStream(TableauMetadataStream):
    name = "custom_sql_locations_metadata"
    schema = th.PropertiesList(
        th.Property("name", th.StringType),
        th.Property("downstreamWorkbooks", th.ArrayType(
            th.ObjectType(
                th.Property("id", th.StringType),
                th.Property("name", th.StringType)
            )
        )),
        th.Property("database", th.ObjectType(
            th.Property("name", th.StringType),
            th.Property("connectionType", th.StringType),
        )),
        th.Property("tables", th.ArrayType(
            th.Property("name", th.StringType),
        )),
        th.Property("query", th.StringType)
    ).to_dict()
    primary_keys = ["name"]
    replication_key = None

    @property
    def query(self) -> str:
        return """
            query listCustomSQLTables {
              customSQLTablesConnection{
                nodes {
                  name
                  downstreamWorkbooks{
                    id
                    name
                  }
                  database {
                    name
                    connectionType
                  } 
                  tables {
                    name
                  }
                  query
                }
              }
            }
        """

    def parse_response(self, response: requests.Response) -> Iterable[dict]:
        resp_json = response.json()
        for row in resp_json["data"]["customSQLTablesConnection"]["nodes"]:
            row["tables"] = [row["name"] for row in row["tables"]]
            yield row
