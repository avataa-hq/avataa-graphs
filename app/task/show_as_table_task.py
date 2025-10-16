from services.graph import GraphService
from task.models.dto import DbTmoNode
from task.models.enums import Status
from task.models.outgoing_data import TmoNodeResponse
from task.task_abstract import TaskAbstract, TaskChecks


class ShowAsATableTask(TaskAbstract, TaskChecks):
    def __init__(
        self,
        graph_db: GraphService,
        key: str,
        node_key: str,
        show_as_a_table: bool,
    ):
        TaskAbstract.__init__(self, graph_db=graph_db, key=key)
        self.node_key = node_key
        self.show_as_a_table = show_as_a_table

    def check(self):
        self.check_status(
            document=self.document, possible_status=[Status.COMPLETE]
        )
        self.check_collection(
            document=self.document, tmo_collection=self.tmo_collection
        )
        self.check_nodes(
            keys=[self.node_key],
            database=self.database,
            main_collection_name=self.tmo_collection.name,
        )
        self.check_global_uniqueness(
            tmo_collection=self.tmo_collection, node_key=self.node_key
        )

    def execute(self):
        db_tmo_node = DbTmoNode.model_validate(
            self.tmo_collection.get({"_key": self.node_key})
        )
        db_tmo_node.show_as_a_table = self.show_as_a_table
        result = self.tmo_collection.update(
            db_tmo_node.model_dump(mode="json", by_alias=True), return_new=True
        )
        result = TmoNodeResponse.model_validate(result["new"])
        return result
