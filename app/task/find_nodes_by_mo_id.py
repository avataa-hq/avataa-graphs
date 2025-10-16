from arango.collection import StandardCollection

from config import GraphDBConfig
from services.graph import GraphService
from services.inventory import InventoryInterface
from task.models.enums import Status
from task.models.errors import NotFound


class FindNodesByMoId:
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
        query = """
            FOR doc IN @@mainCollection
                FILTER doc.data.id == @moId
                RETURN doc

        """
        binds = {
            "@mainCollection": self.config.graph_data_collection_name,
            "moId": self.mo_id,
        }
        for graph in graphs:
            db = self.graph_db.get_database(name=graph["database"])
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
