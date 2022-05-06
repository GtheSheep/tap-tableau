"""Stream type classes for tap-tableau-metadata."""

from pathlib import Path
from typing import Any, Dict, Optional, Union, List, Iterable

import requests
import tableauserverclient as TSC
from singer_sdk import typing as th  # JSON Schema typing helpers
from tableauserverclient.server.endpoint.exceptions import ServerResponseError

from tap_tableau.client import TableauMetadataStream
from tap_tableau.client import TableauStream
from tap_tableau.utils import format_datetime
from tap_tableau.utils import get_permission_details
from tap_tableau.utils import get_user_details


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


class GroupsStream(TableauStream):
    name = "groups"
    primary_keys = ["id"]
    replication_key = None
    schema = th.PropertiesList(
        th.Property("id", th.StringType),
        th.Property("domain_name", th.StringType),
        th.Property("license_mode", th.StringType),
        th.Property("minimum_site_role", th.StringType),
        th.Property("name", th.StringType),
        th.Property("tag_name", th.StringType),
        th.Property("users", th.ArrayType(
            th.ObjectType(
                th.Property("id", th.StringType),
                th.Property("auth_setting", th.StringType),
                th.Property("email", th.StringType),
                th.Property("name", th.StringType),
                th.Property("full_name", th.StringType),
                th.Property("role", th.StringType),
            )
        )),
    ).to_dict()

    def get_records(self, context: Optional[dict]) -> Iterable[dict]:
        """Return a generator of row-type dictionary objects.
        """
        for group in TSC.Pager(self.server_client.groups):
            self.server_client.groups.populate_users(group)
            row = {
                'id': group.id,
                'domain_name': group.domain_name,
                'license_mode': group.license_mode,
                'minimum_site_role': group.minimum_site_role,
                'name': group.name,
                'tag_name': group.tag_name,
                'users': [get_user_details(user) for user in group.users]
            }
            yield row


class ProjectsStream(TableauStream):
    name = "projects"
    primary_keys = ["id"]
    replication_key = None
    schema = th.PropertiesList(
        th.Property("id", th.StringType),
        th.Property("name", th.StringType),
        th.Property("owner_id", th.StringType),
        th.Property("parent_id", th.StringType),
        th.Property("description", th.StringType),
        th.Property("is_default", th.BooleanType),
        th.Property("content_permissions", th.StringType),
        th.Property("default_datasource_permissions", th.ArrayType(
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
        th.Property("default_flow_permissions", th.ArrayType(
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
        th.Property("default_workbook_permissions", th.ArrayType(
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
    ).to_dict()

    def get_records(self, context: Optional[dict]) -> Iterable[dict]:
        """Return a generator of row-type dictionary objects.
        """
        for project in TSC.Pager(self.server_client.projects):
            self.server_client.projects.populate_permissions(project)
            self.server_client.projects.populate_datasource_default_permissions(project)
            self.server_client.projects.populate_flow_default_permissions(project)
            self.server_client.projects.populate_workbook_default_permissions(project)
            row = {
                'content_permissions': project.content_permissions,
                'default_datasource_permissions': [get_permission_details(permission) for permission in project.default_datasource_permissions],
                'default_flow_permissions': [get_permission_details(permission) for permission in project.default_flow_permissions],
                'default_workbook_permissions': [get_permission_details(permission) for permission in project.default_workbook_permissions],
                'description': project.description,
                'id': project.id,
                'is_default': project.is_default(),
                'name': project.name,
                'owner_id': project.owner_id,
                'parent_id': project.parent_id
            }
            yield row


class SchedulesStream(TableauStream):
    name = "schedules"
    primary_keys = ["id"]
    replication_key = None
    schema = th.PropertiesList(
        th.Property("id", th.StringType),
        th.Property("name", th.StringType),
        th.Property("interval_item", th.StringType),
        th.Property("execution_order", th.StringType),
        th.Property("priority", th.NumberType),
        th.Property("schedule_type", th.StringType),
        th.Property("state", th.StringType),
        th.Property("created_at", th.DateTimeType),
        th.Property("end_schedule_at", th.DateTimeType),
        th.Property("next_run_at", th.DateTimeType),
        th.Property("updated_at", th.DateTimeType),
    ).to_dict()

    def get_records(self, context: Optional[dict]) -> Iterable[dict]:
        """Return a generator of row-type dictionary objects.
        """
        for schedule in TSC.Pager(self.server_client.schedules):
            row = {
                'created_at': format_datetime(schedule.created_at),
                'end_schedule_at': format_datetime(schedule.end_schedule_at),
                'execution_order': schedule.execution_order,
                'id': schedule.id,
                'interval_item': schedule.interval_item,
                'name': schedule.name,
                'next_run_at': format_datetime(schedule.next_run_at),
                'priority': schedule.priority,
                'schedule_type': schedule.schedule_type,
                'state': schedule.state,
                'updated_at': format_datetime(schedule.updated_at),
            }
            yield row


class TasksStream(TableauStream):
    name = "tasks"
    primary_keys = ["id"]
    replication_key = None
    schema = th.PropertiesList(
        th.Property("id", th.StringType),
        th.Property("task_type", th.StringType),
        th.Property("schedule_id", th.StringType),
        th.Property("priority", th.NumberType),
        th.Property("last_run_at", th.DateTimeType),
        th.Property("consecutive_failed_count", th.NumberType),
        th.Property("target", th.ObjectType(
            th.Property("id", th.StringType),
            th.Property("type", th.StringType),
        )),
    ).to_dict()

    def get_records(self, context: Optional[dict]) -> Iterable[dict]:
        """Return a generator of row-type dictionary objects.
        """
        for task in TSC.Pager(self.server_client.tasks):
            row = {
                'consecutive_failed_count': task.consecutive_failed_count,
                'id': task.id,
                'last_run_at': format_datetime(task.last_run_at),
                'priority': task.priority,
                'schedule_id': task.schedule_id,
                'target': {
                    'id': task.target.id,
                    'type': task.target.type
                },
                'task_type': task.task_type
            }
            yield row


class WorkbooksStream(TableauStream):
    name = "workbooks"
    primary_keys = ["id"]
    replication_key = None
    schema = th.PropertiesList(
        th.Property("id", th.StringType),
        th.Property("name", th.StringType),
        th.Property("content_url", th.StringType),
        th.Property("created_at", th.DateTimeType),
        th.Property("data_acceleration_config", th.ObjectType(
            th.Property("accelerate_now", th.BooleanType),
            th.Property("acceleration_enabled", th.BooleanType),
            th.Property("acceleration_status", th.StringType),
            th.Property("last_updated_at", th.DateTimeType),
        )),
        th.Property("description", th.StringType),
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
        th.Property("show_tabs", th.BooleanType),
        th.Property("size", th.NumberType),
        th.Property("tags", th.ArrayType(th.StringType)),
        th.Property("updated_at", th.DateTimeType),
        th.Property("webpage_url", th.StringType),
    ).to_dict()

    def get_records(self, context: Optional[dict]) -> Iterable[dict]:
        """Return a generator of row-type dictionary objects.
        """
        for workbook in TSC.Pager(self.server_client.workbooks):
            self.server_client.workbooks.populate_connections(workbook)
            self.server_client.workbooks.populate_permissions(workbook)
            self.server_client.workbooks.populate_views(workbook)
            try:
                permissions = [get_permission_details(permission) for permission in workbook.permissions]
            except ServerResponseError:
                permissions = []
            row = {
                'content_url': workbook.content_url,
                'created_at': format_datetime(workbook.created_at),
                'data_acceleration_config': workbook.data_acceleration_config,
                'description': workbook.description,
                'id': workbook.id,
                'name': workbook.name,
                'owner_id': workbook.owner_id,
                'permissions': permissions,
                'project_id': str(workbook.project_id),
                'project_name': workbook.project_name,
                'show_tabs': workbook.show_tabs,
                'size': workbook.size,
                'tags': list(workbook.tags),
                'updated_at': format_datetime(workbook.updated_at),
                'webpage_url': workbook.webpage_url
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
        th.Property("id", th.StringType),
        th.Property("name", th.StringType),
        th.Property("downstreamWorkbooks", th.ArrayType(
            th.ObjectType(
                th.Property("id", th.StringType),
                th.Property("name", th.StringType)
            )
        )),
        th.Property("query", th.StringType)
    ).to_dict()
    primary_keys = ["id"]
    replication_key = None

    @property
    def query(self) -> str:
        return """
            query listCustomSQLTables {
                customSQLTables {
                  name
                  id
                  query
                  downstreamWorkbooks {
                   id
                   name
                  }
               }
            }
        """

    def parse_response(self, response: requests.Response) -> Iterable[dict]:
        resp_json = response.json()
        for row in resp_json["data"]["customSQLTables"]:
            yield row


class UsersMetadataStream(TableauMetadataStream):
    name = "users_metadata"
    schema = th.PropertiesList(
        th.Property("id", th.StringType),
        th.Property("name", th.StringType),
    ).to_dict()
    primary_keys = ["id"]
    replication_key = None

    @property
    def query(self) -> str:
        return """
            query users {
                tableauUsers {
                    name
                    id
                }
            }
        """

    def parse_response(self, response: requests.Response) -> Iterable[dict]:
        resp_json = response.json()
        for row in resp_json["data"]["tableauUsers"]:
            yield row
