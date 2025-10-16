from arango.collection import StandardCollection
import deprecation

from config import GraphDBConfig
from services.graph import GraphService
from services.inventory import InventoryInterface
from task.models.enums import ConnectionType, Status
from task.models.errors import NotFound


@deprecation.deprecated(details="Use FindTraceNodesByMoIds")
class FindTraceNodesByMoId:
    def __init__(
        self,
        graph_db: GraphService,
        inventory: InventoryInterface,
        mo_id: int,
    ):
        self.graph_db = graph_db
        self.inventory = inventory
        self.mo_id = mo_id
        self._tmo_id = None

        self.config = GraphDBConfig()
        self.main_collection: StandardCollection = self.graph_db.get_collection(
            db=self.config.sys_database_name,
            name=self.config.main_graph_collection_name,
        )

    def tmo_id(self) -> int:
        if not self._tmo_id:
            try:
                self._tmo_id = self.inventory.get_tmo_by_mo_id(self.mo_id)
            except ValueError:
                pass
        return self._tmo_id

    def check(self):
        if not self.tmo_id():
            raise NotFound("TMO not found")

    def get_graphs_with_tmo_id(self):
        query = """
            FOR doc IN @@mainCollection
                FILTER doc.status == @status
                FILTER @tmoId IN doc.active_tmo_ids
                RETURN doc
        """
        tmo_id = self.tmo_id()
        binds = {
            "@mainCollection": self.config.main_graph_collection_name,
            "tmoId": tmo_id,
            "status": Status.COMPLETE.value,
        }
        response = list(
            self.graph_db.sys_db.aql.execute(query=query, bind_vars=binds)
        )
        return response

    def find_mo_id_in_graphs(self, graphs: list[dict]):
        results = {}  # {db_key: list[doc]}
        query_is_trace_item = f"""
            FOR doc in @@mainCollection
                FILTER doc.tmo == @traceTmoId
                FILTER doc.data.id == @moId
                FOR v, e IN 1..1 INBOUND doc._id GRAPH @mainGraph
                    FILTER e.connection_type
                        IN ["{ConnectionType.MO_LINK.value}", "{ConnectionType.TWO_WAY_MO_LINK.value}"]
                    LIMIT 1
                    RETURN DISTINCT doc
        """
        query = f"""
            FOR doc IN @@mainCollection
                FILTER doc.data.id == @moId
                FOR v, e IN 1..1 OUTBOUND doc._id GRAPH @mainGraph
                    FILTER e.connection_type
                    IN ["{ConnectionType.MO_LINK.value}", "{ConnectionType.TWO_WAY_MO_LINK.value}"]
                    FILTER v.tmo == @traceTmoId
                    LIMIT 1
                    RETURN DISTINCT doc
        """
        binds = {
            "@mainCollection": self.config.graph_data_collection_name,
            "moId": self.mo_id,
            "mainGraph": self.config.graph_data_graph_name,
        }
        for graph in graphs:
            db = self.graph_db.get_database(name=graph["database"])
            trace = self.graph_db.get_collection(
                db=db, name=self.config.config_collection_name
            ).get("trace_tmo_id")
            if not trace:
                continue
            trace_tmo_id = trace["tmo_id"]
            binds["traceTmoId"] = trace_tmo_id
            docs = list(
                db.aql.execute(query=query_is_trace_item, bind_vars=binds)
            )
            if not docs:
                docs = list(db.aql.execute(query=query, bind_vars=binds))
            if not docs:
                continue
            results[graph["_key"]] = docs
        graphs_dict = {i["_key"]: i for i in graphs}
        formatted_results = []
        for db_key, nodes in results.items():
            graph = graphs_dict[db_key]
            formatted_result = {
                "key": db_key,
                "name": graph["name"],
                "nodes": nodes,
            }
            formatted_results.append(formatted_result)
        return formatted_results

    def execute(self):
        graphs = self.get_graphs_with_tmo_id()
        graph_data = self.find_mo_id_in_graphs(graphs=graphs)
        return graph_data


class FindTraceNodesByMoIds:
    def __init__(
        self,
        graph_db: GraphService,
        inventory: InventoryInterface,
        mo_ids: list[int],
    ):
        self.graph_db = graph_db
        self.inventory = inventory
        self.mo_ids = mo_ids
        self._tmo_ids = None

        self.config = GraphDBConfig()
        self.main_collection: StandardCollection = self.graph_db.get_collection(
            db=self.config.sys_database_name,
            name=self.config.main_graph_collection_name,
        )

    def tmo_ids(self) -> list[int]:
        if not self._tmo_ids:
            try:
                self._tmo_ids = [
                    int(i["tmo_id"])
                    for i in self.inventory.get_mos_by_mo_ids(self.mo_ids)
                ]
            except ValueError:
                pass
        return self._tmo_ids

    def check(self):
        if not self.tmo_ids():
            raise NotFound("TMO not found")

    def get_graphs_with_tmo_ids(self):
        query = """
            FOR doc IN @@mainCollection
                FILTER doc.status == @status
                FILTER @tmoIds ANY IN doc.active_tmo_ids
                RETURN doc
        """
        tmo_ids = self.tmo_ids()
        binds = {
            "@mainCollection": self.config.main_graph_collection_name,
            "tmoIds": tmo_ids,
            "status": Status.COMPLETE.value,
        }
        response = list(
            self.graph_db.sys_db.aql.execute(query=query, bind_vars=binds)
        )
        return response

    def find_mo_ids_in_graphs(self, graphs: list[dict]):
        results = {}  # {db_key: list[doc]}
        query_is_trace_item = f"""
            FOR doc in @@mainCollection
                FILTER doc.tmo == @traceTmoId
                FILTER doc.data.id IN @moIds
                FOR v, e IN 1..1 INBOUND doc._id GRAPH @mainGraph
                    FILTER e.connection_type
                        IN ["{ConnectionType.MO_LINK.value}", "{ConnectionType.TWO_WAY_MO_LINK.value}"]
                    RETURN DISTINCT doc
        """
        query = f"""
            FOR doc IN @@mainCollection
                FILTER doc.data.id IN @moIds
                FOR v, e IN 1..1 OUTBOUND doc._id GRAPH @mainGraph
                    FILTER e.connection_type
                    IN ["{ConnectionType.MO_LINK.value}", "{ConnectionType.TWO_WAY_MO_LINK.value}"]
                    FILTER v.tmo == @traceTmoId
                    RETURN DISTINCT doc
        """
        binds = {
            "@mainCollection": self.config.graph_data_collection_name,
            "moIds": self.mo_ids,
            "mainGraph": self.config.graph_data_graph_name,
        }
        for graph in graphs:
            db = self.graph_db.get_database(name=graph["database"])
            trace = self.graph_db.get_collection(
                db=db, name=self.config.config_collection_name
            ).get("trace_tmo_id")
            if not trace:
                continue
            trace_tmo_id = trace["tmo_id"]
            binds["traceTmoId"] = trace_tmo_id
            docs = list(
                db.aql.execute(query=query_is_trace_item, bind_vars=binds)
            )
            if not docs:
                docs = list(db.aql.execute(query=query, bind_vars=binds))
            if not docs:
                continue
            results[graph["_key"]] = docs
        graphs_dict = {i["_key"]: i for i in graphs}
        formatted_results = []
        for db_key, nodes in results.items():
            graph = graphs_dict[db_key]
            formatted_result = {
                "key": db_key,
                "name": graph["name"],
                "nodes": nodes,
            }
            formatted_results.append(formatted_result)
        return formatted_results

    def execute(self):
        graphs = self.get_graphs_with_tmo_ids()
        graph_data = self.find_mo_ids_in_graphs(graphs=graphs)
        return graph_data
