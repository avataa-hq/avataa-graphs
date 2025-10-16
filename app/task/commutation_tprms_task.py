from arango import DocumentRevisionError, DocumentUpdateError

from services.graph import GraphService
from task.models.dto import DbTmoNode
from task.models.errors import ValidationError
from task.models.outgoing_data import TmoNodeResponse
from task.task_abstract import TaskAbstract, TaskChecks


class SetCommutationTprmsTask(TaskAbstract, TaskChecks):
    def __init__(
        self,
        graph_db: GraphService,
        key: str,
        node_key: str,
        tprm_ids: list[int] | None,
    ):
        TaskAbstract.__init__(self, graph_db=graph_db, key=key)
        self._node_key = node_key
        self._tprm_ids = list(set(tprm_ids)) if tprm_ids else None

    def check(self):
        self.check_collection(
            document=self.document, tmo_collection=self.tmo_collection
        )
        self.check_nodes(
            keys=[self._node_key],
            database=self.database,
            main_collection_name=self.tmo_collection.name,
        )
        self.check_commutation_tprms(
            tmo_id=int(self._node_key),
            database=self.database,
            tprm_ids=self._tprm_ids,
            tmo_collection_name=self.tmo_collection.name,
        )

    def execute(self):
        node = self.tmo_collection.get({"_key": self._node_key})
        node = DbTmoNode.model_validate(node)
        node.commutation_tprms = self._tprm_ids
        try:
            response = self.tmo_collection.update(
                node.model_dump(by_alias=True, mode="json"), return_new=True
            )
        except DocumentRevisionError | DocumentUpdateError:
            raise ValidationError("Cannot update commutation tprms")
        else:
            updated_tmo = TmoNodeResponse.model_validate(response["new"])
            return updated_tmo
