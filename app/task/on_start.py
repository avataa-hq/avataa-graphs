from arango.collection import StandardCollection
from arango.database import StandardDatabase

from config import GraphDBConfig
from services.graph import GraphService, IfNotExistType
from task.models.enums import Status


class OnStartTask:
    def __init__(self, graphdb: GraphService):
        self.graph_db = graphdb
        self.config = GraphDBConfig()
        self.main_db: StandardDatabase = self.graph_db.get_database(
            name=self.config.sys_database_name,
            if_not_exist=IfNotExistType.CREATE,
        )
        self.main_collection: StandardCollection = self.graph_db.get_collection(
            db=self.config.sys_database_name,
            name=self.config.main_graph_collection_name,
            if_not_exist=IfNotExistType.CREATE,
        )

    def _find_uncompleted_graphs(self):
        query = """
            FOR doc in @@mainCollection
                FILTER doc.status == @status
                RETURN doc
        """
        binds = {
            "@mainCollection": self.config.main_graph_collection_name,
            "status": Status.IN_PROCESS.value,
        }
        response = list(self.main_db.aql.execute(query=query, bind_vars=binds))
        return response

    @staticmethod
    def _mark_graph_error(items: list):
        if not items:
            return
        modified_items = []
        for i in items:
            new_item = i.copy()
            new_item["status"] = Status.ERROR.value
            new_item["error_description"] = (
                "The microservice terminated unexpectedly during the process"
            )
            modified_items.append(new_item)
        return modified_items

    def _update_items(self, items: list):
        if not items:
            return
        self.main_collection.update_many(items, raise_on_document_error=True)

    def execute(self):
        uncompleted_graphs = self._find_uncompleted_graphs()
        marked_graphs = self._mark_graph_error(items=uncompleted_graphs)
        self._update_items(items=marked_graphs)
