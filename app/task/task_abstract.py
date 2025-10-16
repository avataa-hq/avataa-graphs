from abc import ABC

from arango.collection import StandardCollection
from arango.database import StandardDatabase
from fastapi import HTTPException
from starlette.status import HTTP_510_NOT_EXTENDED

from config import GraphDBConfig
from services.graph import GraphService, IfNotExistType
from task.models.dto import DbMainRecord, DbTmoNode
from task.models.enums import ConnectionType, Status
from task.models.errors import (
    DocumentNotFound,
    NotFound,
    StartNodeNotFound,
    StatusError,
    TraceNodeNotFound,
    ValidationError,
)
from task.models.outgoing_data import NodeEdgeErrorResponse, TmoUpdate


class TaskAbstract(ABC):
    def __init__(
        self,
        graph_db: GraphService,
        key: str,
    ):
        self.graph_db = graph_db
        self.key = key
        self.config = GraphDBConfig()

        self._system_main_collection: StandardCollection | None = None
        self._document: DbMainRecord | None = None
        self._database: StandardDatabase | None = None
        self._tmo_collection: StandardCollection | None = None
        self._tmo_edge_collection: StandardCollection | None = None
        self._main_collection: StandardCollection | None = None
        self._main_edge_collection: StandardCollection | None = None
        self._main_path_collection: StandardCollection | None = None
        self._config_collection: StandardCollection | None = None
        self._trace_tmo_id: int | None = None
        self._trace_tprm_id: int | None = None
        self._group_by_tprm_ids: list[int] | None = None
        self._delete_orphan_branches: bool | None = None
        self._trace_tmo_data: DbTmoNode | None = None
        self._start_from_tmo: int | None = None
        self._start_from_tprm: int | None = None

    @property
    def sys_db(self) -> StandardDatabase:
        return self.graph_db.sys_db

    @property
    def system_main_collection(self) -> StandardCollection:
        if self._system_main_collection is None:
            self._system_main_collection = self.graph_db.get_collection(
                db=self.config.sys_database_name,
                name=self.config.main_graph_collection_name,
                if_not_exist=IfNotExistType.CREATE,
            )
        return self._system_main_collection

    @property
    def main_collection(self) -> StandardCollection:
        if self._main_collection is None:
            self._main_collection = self.graph_db.get_collection(
                db=self.database,
                name=self.config.graph_data_collection_name,
                if_not_exist=IfNotExistType.CREATE,
            )
        return self._main_collection

    @property
    def main_edge_collection(self) -> StandardCollection:
        if self._main_edge_collection is None:
            self._main_edge_collection = self.graph_db.get_collection(
                db=self.database,
                name=self.config.graph_data_edge_name,
                if_not_exist=IfNotExistType.CREATE,
                edge=True,
            )
        return self._main_edge_collection

    @property
    def main_path_collection(self) -> StandardCollection:
        if self._main_path_collection is None:
            self._main_path_collection = self.graph_db.get_collection(
                db=self.database,
                name=self.config.graph_data_path_name,
                if_not_exist=IfNotExistType.CREATE,
                edge=True,
            )
        return self._main_path_collection

    @property
    def tmo_collection(self) -> StandardCollection:
        if self._tmo_collection is None:
            self._tmo_collection = self.graph_db.get_collection(
                db=self.database, name=self.config.tmo_collection_name
            )
        return self._tmo_collection

    @property
    def tmo_edge_collection(self) -> StandardCollection:
        if self._tmo_edge_collection is None:
            self._tmo_edge_collection = self.graph_db.get_collection(
                db=self.database, name=self.config.tmo_edge_name
            )
        return self._tmo_edge_collection

    @property
    def config_collection(self) -> StandardCollection:
        if self._config_collection is None:
            self._config_collection = self.graph_db.get_collection(
                db=self.database,
                name=self.config.config_collection_name,
                if_not_exist=IfNotExistType.CREATE,
            )
        return self._config_collection

    @property
    def document(self) -> DbMainRecord:
        if self._document is None:
            response = self.system_main_collection.get(document=self.key)
            if not response:
                raise DocumentNotFound(
                    f"Document with key {self.key} not found"
                )
            self._document = DbMainRecord(**response)
        return self._document

    @property
    def database(self) -> StandardDatabase:
        if self._database is None:
            db_name = self.document.database
            self._database = self.graph_db.get_database(
                name=db_name, if_not_exist=IfNotExistType.RAISE_ERROR
            )
        return self._database

    @property
    def trace_tmo_id(self) -> int | None:
        if self._trace_tmo_id is None:
            doc = self.config_collection.get("trace_tmo_id")
            if doc:
                self._trace_tmo_id = doc["tmo_id"]
        return self._trace_tmo_id

    @property
    def trace_tprm_id(self) -> int | None:
        if self._trace_tprm_id is None:
            doc = self.config_collection.get("trace_tprm_id")
            if doc:
                self._trace_tprm_id = doc["tprm_id"]
        return self._trace_tprm_id

    @property
    def group_by_tprm_ids(self) -> list[int] | None:
        if self._group_by_tprm_ids is None:
            doc = self.config_collection.get("group_by")
            if doc:
                self._group_by_tprm_ids = doc["tprms"]
        return self._group_by_tprm_ids

    @property
    def delete_orphan_branches_status(self) -> bool:
        if self._delete_orphan_branches is None:
            doc = self.config_collection.get("delete_orphan_branches")
            if doc:
                self._delete_orphan_branches = doc["delete_orphan_branches"]
            else:
                self._delete_orphan_branches = False
        return self._delete_orphan_branches

    @property
    def trace_tmo_data(self) -> DbTmoNode | None:
        if self._trace_tmo_data is None and self.trace_tmo_id:
            raw = self.tmo_collection.get({"_key": str(self.trace_tmo_id)})
            self._trace_tmo_data = DbTmoNode.model_validate(raw)
        return self._trace_tmo_data

    @property
    def start_from_tmo(self) -> int | None:
        if not self._start_from_tmo:
            start_from = self.config_collection.get({"_key": "start_from"})
            tmo_id = (
                start_from.get("tmo_id", self.document.tmo_id)
                if start_from
                else self.document.tmo_id
            )
            self._start_from_tmo = tmo_id
        return self._start_from_tmo

    @property
    def start_from_tprm(self) -> int | None:
        if not self._start_from_tprm:
            start_from = self.config_collection.get({"_key": "start_from"})
            tprm_id = start_from.get("tprm_id", None) if start_from else None
            self._start_from_tprm = tprm_id
        return self._start_from_tprm

    def _get_tmos_data(self, tmo_ids: list[int]) -> list[DbTmoNode]:
        if not tmo_ids:
            return []
        query = """
            FOR doc IN @@tmoCollection
                FILTER doc._key IN @tmoIds
                RETURN doc
        """
        binds = {
            "@tmoCollection": self.config.tmo_collection_name,
            "tmoIds": [str(i) for i in tmo_ids],
        }
        response = self.database.aql.execute(query=query, bind_vars=binds)
        results = [DbTmoNode.model_validate(i) for i in response]
        return results


class TaskWithMaxSize(ABC):
    def __init__(self, response_length: int):
        self._response_length = response_length

    def check_response_length(self, response):
        try:
            response_length = (
                response.size if hasattr(response, "size") else len(response)
            )
        except TypeError:
            response_length = 1
        if 0 < self._response_length < response_length:
            error_response = NodeEdgeErrorResponse(
                description="Response size exceeded. Specify your request parameters",
                params={
                    "size": response_length,
                    "max_size": self._response_length,
                },
            )
            raise HTTPException(
                status_code=HTTP_510_NOT_EXTENDED,
                detail=error_response.model_dump(mode="json"),
            )
        return response


class TaskChecks:
    @staticmethod
    def check_collection(
        tmo_collection: StandardCollection, document: DbMainRecord
    ):
        query = {"_key": str(document.tmo_id)}
        start_node = tmo_collection.get(document=query)
        if not start_node:
            raise StartNodeNotFound("Start node not found")

    @staticmethod
    def check_status(
        document: DbMainRecord,
        possible_status: list[Status] | None = None,
        impossible_status: list[Status] | None = None,
    ):
        if possible_status == impossible_status:
            raise ValueError(f"{possible_status} is equal {impossible_status=}")
        if possible_status and document.status not in possible_status:
            raise StatusError(
                f"The status {document.status.value} does not allow this operation to be performed"
            )
        if impossible_status and document.status in impossible_status:
            raise StatusError(
                f"The status {document.status.value} does not allow this operation to be performed"
            )

    @staticmethod
    def check_nodes(
        keys: list[str], main_collection_name: str, database: StandardDatabase
    ):
        if not keys:
            return
        query = """
            FOR doc IN @@mainCollection
                FILTER doc._key IN @keys
                COLLECT WITH COUNT INTO length
                RETURN length
        """
        binds = {
            "@mainCollection": main_collection_name,
            "keys": keys,
        }
        response = next(database.aql.execute(query=query, bind_vars=binds))
        if response != len(keys):
            raise NotFound("Nodes not found in database")

    @staticmethod
    def check_edges(
        keys: list[str],
        main_edge_collection_name: str,
        database: StandardDatabase,
    ):
        if not keys:
            return
        query = """
            FOR doc in @@mainEdgeCollection
                FILTER doc._key IN @keys
                COLLECT WITH COUNT INTO length
                RETURN length
        """
        binds = {
            "@mainEdgeCollection": main_edge_collection_name,
            "keys": keys,
        }
        response = next(database.aql.execute(query=query, bind_vars=binds))
        if response != len(set(keys)):
            raise NotFound("Edges not found in database")

    @staticmethod
    def check_view_exists(database: StandardDatabase):
        view = database.view("search-view")
        if not view:
            raise NotFound("Search indexes not found. Please rebuild the graph")

    @staticmethod
    def check_trace_tmo_id(tmo_id: int | None):
        if not tmo_id:
            raise TraceNodeNotFound("The Trace TMO ID not set")

    @staticmethod
    def check_group_by(
        data,
        start_from: int,
        tmo_graph_name: str,
        tmo_collection_name: str,
        database: StandardDatabase,
    ):
        if not data.group_by_tprms:
            return
        query = """
            LET tprms = @tprms

            FOR tprm in tprms
                FOR doc IN @@tmoCollection
                    FILTER doc.params[*].id ANY == tprm

                    LET path = (
                        FOR v, e IN ANY SHORTEST_PATH
                            doc._id TO @startFrom
                            GRAPH @tmoGraph

                            FILTER v[*].enabled ALL == True
                            FILTER e[*].enabled ALL == True
                            FILTER e[*].link_type ALL == "p_id"

                            RETURN e
                        )
                    RETURN LENGTH(path)
        """
        binds = {
            "tprms": data.group_by_tprms,
            "startFrom": start_from,
            "tmoGraph": tmo_graph_name,
            "@tmoCollection": tmo_collection_name,
        }
        response = list(database.aql.execute(query=query, bind_vars=binds))
        if len(response) != len(data.group_by_tprms):
            raise NotFound("Tprm not found")
        if not all(
            response[i] <= response[i + 1] for i in range(len(response) - 1)
        ):
            raise ValidationError("The order of the tprms is out of order")

    @staticmethod
    def check_start_from(
        data,
        database: StandardDatabase,
        settings_collection: StandardCollection,
        tmo_collection_name: str,
    ):
        if not data.start_from_tmo_id:
            if data.start_from_tprm_id:
                raise ValidationError("TPRM ID must be used only with TMO ID")
            return
        query = """
            FOR doc IN @@tmoCollection
                FILTER doc.id == @tmoId
                RETURN doc
        """
        binds = {
            "@tmoCollection": tmo_collection_name,
            "tmoId": data.start_from_tmo_id,
        }
        response = list(database.aql.execute(query=query, bind_vars=binds))
        if len(response) == 0:
            raise NotFound("TMO ID not found")

        if data.start_from_tprm_id:
            query = """
                FOR doc IN @@tmoCollection
                    FILTER doc.id == @tmoId
                    FILTER doc.params[*].id ANY == @tprmId
                    RETURN doc
            """
            binds = {
                "@tmoCollection": tmo_collection_name,
                "tmoId": data.start_from_tmo_id,
                "tprmId": data.start_from_tprm_id,
            }

            response = list(database.aql.execute(query=query, bind_vars=binds))
            if len(response) == 0:
                raise NotFound("TPRM ID not found or refers to another TMO")

            group_by_tprms = data.group_by_tprms
            if not group_by_tprms:
                group_by_from_db = settings_collection.get("group_by")
                if group_by_from_db:
                    group_by_from_db = group_by_from_db.get("tprms", [])
                else:
                    group_by_from_db = []
                group_by_tprms = group_by_from_db
            group_by_tprms = set(group_by_tprms)
            if data.start_from_tprm_id not in group_by_tprms:
                raise ValidationError(
                    "The parameter type must be specified in the grouping"
                )

    @staticmethod
    def check_trace(
        data: TmoUpdate,
        document: DbMainRecord,
        tmo_collection: StandardCollection,
    ):
        if not data.trace_tmo_id:
            return
        if document.tmo_id == data.trace_tmo_id:
            raise ValidationError(
                "Trace ID cannot be equal to the starting TMO ID element"
            )
        tmo_node = tmo_collection.get(str(data.trace_tmo_id))
        if not tmo_node:
            raise ValidationError("Trace ID not found in TMO IDs list")
        if data.trace_tprm_id:
            tmo_node = DbTmoNode.model_validate(tmo_node)
            for param in tmo_node.params:
                if param.id == data.trace_tprm_id:
                    break
            else:
                raise ValidationError("Trace TPRM ID not found")

    @staticmethod
    def check_commutation_tprms(
        tmo_id: int,
        tprm_ids: list[int] | None,
        database: StandardDatabase,
        tmo_collection_name: str,
    ):
        if not tprm_ids:
            return
        query = f"""
            FOR doc IN @@tmoCollection
                FILTER doc.id == @tmoId
                FILTER doc.global_uniqueness == false
                FILTER NOT_NULL(doc.params)
                LIMIT 1
                FOR param in doc.params
                    FILTER param.val_type
                        IN ["{ConnectionType.MO_LINK.value}", "{ConnectionType.TWO_WAY_MO_LINK.value}"]
                    FILTER param.id IN @tprmIds
                    COLLECT WITH COUNT INTO length
                    RETURN length
        """
        binds = {
            "tmoId": tmo_id,
            "tprmIds": tprm_ids,
            "@tmoCollection": tmo_collection_name,
        }
        response = list(database.aql.execute(query=query, bind_vars=binds))
        if len(set(tprm_ids)) != response[0]:
            raise ValidationError("Wrong TPRM ids")

    @staticmethod
    def check_busy_param_uniqueness(busy_parameter_groups: list[list[int]]):
        passed = set()
        for group in busy_parameter_groups:
            if passed.intersection(group):
                raise ValidationError("TPRM id must be unique")
            passed.update(group)

    @staticmethod
    def check_global_uniqueness(
        tmo_collection: StandardCollection, node_key: str
    ):
        node = tmo_collection.get({"_key": node_key})
        node = DbTmoNode.model_validate(node)
        if node.global_uniqueness:
            raise ValidationError(
                "The value can only be set for TMOs with non-global uniqueness"
            )
