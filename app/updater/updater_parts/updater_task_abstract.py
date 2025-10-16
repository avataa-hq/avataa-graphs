from arango.collection import StandardCollection
from arango.database import StandardDatabase

from services.graph import GraphService, IfNotExistType
from task.models.dto import DbMainRecord, DbTmoNode
from task.models.errors import (
    DocumentNotFound,
)
from task.task_abstract import TaskAbstract


class UpdaterTaskAbstract(TaskAbstract):
    def __init__(
        self,
        graph_db: GraphService,
        key: str,
    ):
        super().__init__(graph_db=graph_db, key=key)

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
        response = self.system_main_collection.get(document=self.key)
        if not response:
            raise DocumentNotFound(f"Document with key {self.key} not found")
        return DbMainRecord.model_validate(response)

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
        doc = self.config_collection.get("trace_tmo_id")
        if doc:
            return doc["tmo_id"]

    @property
    def group_by_tprm_ids(self) -> list[int] | None:
        doc = self.config_collection.get("group_by")
        if doc:
            return doc["tprms"]

    @property
    def delete_orphan_branches_status(self) -> bool:
        doc = self.config_collection.get("delete_orphan_branches")
        if doc:
            return doc["delete_orphan_branches"]
        else:
            return False

    @property
    def trace_tmo_data(self) -> DbTmoNode | None:
        trace_tmo_id = self.trace_tmo_id
        if not trace_tmo_id:
            return None
        raw = self.tmo_collection.get({"_key": str(trace_tmo_id)})
        return DbTmoNode.model_validate(raw)

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

    @property
    def start_from_tmo(self) -> int | None:
        start_from = self.config_collection.get({"_key": "start_from"})
        return (
            start_from.get("tmo_id", self.document.tmo_id)
            if start_from
            else self.document.tmo_id
        )

    @property
    def start_from_tprm(self) -> int | None:
        start_from = self.config_collection.get({"_key": "start_from"})
        return start_from.get("tprm_id", None) if start_from else None

    @property
    def trace_tprm_id(self) -> int | None:
        doc = self.config_collection.get("trace_tprm_id")
        if doc:
            return doc["tprm_id"]
