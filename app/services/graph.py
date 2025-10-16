from enum import IntFlag, auto
from sys import stderr
import traceback

from arango import (
    ArangoClient,
    CollectionDeleteError,
    DatabaseDeleteError,
    DefaultHTTPClient,
    GraphCreateError,
    ServerConnectionError,
)
from arango.collection import StandardCollection
from arango.database import StandardDatabase, TransactionDatabase
import requests


class IfNotExistType(IntFlag):
    CREATE = auto()
    RAISE_ERROR = auto()
    RETURN_NONE = auto()


class GraphService:
    REQUEST_TIMEOUT = 60 * 10

    def __init__(
        self,
        url: str,
        username: str,
        password: str,
        sys_database_name: str = "_system",
    ):
        self._username: str = username
        self._password: str = password
        self._client: ArangoClient = self._init_arango_db(url=url)
        self.sys_db: StandardDatabase = self._init_sys_db(
            sys_database_name=sys_database_name,
            username=username,
            password=password,
        )

    def _init_arango_db(self, url: str) -> ArangoClient:
        return ArangoClient(
            hosts=url,
            http_client=DefaultHTTPClient(request_timeout=self.REQUEST_TIMEOUT),
            request_timeout=self.REQUEST_TIMEOUT,
        )

    def _init_sys_db(
        self, sys_database_name: str, username: str, password: str
    ) -> StandardDatabase:
        try:
            return self._client.db(
                sys_database_name,
                username=username,
                password=password,
                verify=True,
            )
        except ServerConnectionError as e:
            print(f"{self._client.hosts=}\n{sys_database_name=}\n{username=}")
            raise e

    def get_database(
        self, name, if_not_exist: IfNotExistType = IfNotExistType.RAISE_ERROR
    ) -> StandardDatabase | None:
        try:
            if self.sys_db.has_database(name=name):
                return self._client.db(
                    name,
                    username=self._username,
                    password=self._password,
                    verify=True,
                )
        except KeyError as ex:
            print(ex)
            print(name)
        except requests.exceptions.RequestException as ex:
            print(ex)
            print(f"Request details: {ex.request}")
            print(f"Response details: {ex.response}")
        match if_not_exist:
            case IfNotExistType.CREATE:
                if not self.sys_db.create_database(name):
                    raise ValueError("DB with name {} not created".format(name))
                return self._client.db(
                    name,
                    username=self._username,
                    password=self._password,
                    verify=True,
                )
            case IfNotExistType.RAISE_ERROR:
                raise ValueError("DB with name {} not exists".format(name))
            case IfNotExistType.RETURN_NONE:
                return None

    def delete_database(self, name: str) -> bool:
        if name == self.sys_db.db_name:
            return False
        try:
            self.sys_db.delete_database(name, ignore_missing=True)
        except DatabaseDeleteError:
            print(traceback.format_exc(), file=stderr)
            return False
        return True

    def get_collection(
        self,
        db: StandardDatabase | TransactionDatabase | str,
        name: str,
        if_not_exist: IfNotExistType = IfNotExistType.RAISE_ERROR,
        **kwargs,
    ) -> StandardCollection | None:
        if isinstance(db, str):
            db = self.get_database(name=db, **kwargs)
        if db.has_collection(name=name):
            return db.collection(name)
        match if_not_exist:
            case IfNotExistType.CREATE:
                return db.create_collection(name, **kwargs)
            case IfNotExistType.RAISE_ERROR:
                raise ValueError(
                    "Collection with name {} not exists".format(name)
                )
            case IfNotExistType.RETURN_NONE:
                return None

    @staticmethod
    def delete_collection(
        db: StandardDatabase | TransactionDatabase, name: str
    ) -> bool:
        try:
            db.delete_collection(name, ignore_missing=True)
        except CollectionDeleteError:
            print(traceback.format_exc(), file=stderr)
            return False
        return True

    def create_graph(
        self,
        db: StandardDatabase | TransactionDatabase | str,
        name: str,
        edge_collection: str,
        from_vertex_collections: list[str],
        to_vertex_collections: list[str],
        if_exist: IfNotExistType = IfNotExistType.RAISE_ERROR,
    ):
        if isinstance(db, str):
            db = self.get_database(name=name)
        edge_definition = {
            "edge_collection": edge_collection,
            "from_vertex_collections": from_vertex_collections,
            "to_vertex_collections": to_vertex_collections,
        }
        try:
            return db.create_graph(
                name=name, edge_definitions=[edge_definition]
            )
        except GraphCreateError as e:
            if if_exist == IfNotExistType.RAISE_ERROR:
                raise ValueError(str(e)) from e
            if if_exist == IfNotExistType.RETURN_NONE:
                return None
