from arango import DocumentRevisionError, DocumentUpdateError

from services.graph import GraphService
from task.models.dto import DbTmoNode
from task.models.errors import ValidationError
from task.models.outgoing_data import TmoNodeResponse
from task.task_abstract import TaskAbstract, TaskChecks


class SetBusyParametersTask(TaskAbstract, TaskChecks):
    def __init__(
        self,
        graph_db: GraphService,
        key: str,
        node_key: str,
        busy_parameter_groups: list[list[int]] | None,
    ):
        TaskAbstract.__init__(self, graph_db=graph_db, key=key)
        self._node_key = node_key
        self._busy_parameter_groups = (
            busy_parameter_groups if busy_parameter_groups else []
        )

    def check(self):
        self.check_collection(
            document=self.document, tmo_collection=self.tmo_collection
        )
        self.check_nodes(
            keys=[self._node_key],
            database=self.database,
            main_collection_name=self.tmo_collection.name,
        )
        flat_busy_parameters = []
        for busy_parameter_group in self._busy_parameter_groups:
            flat_busy_parameters.extend(busy_parameter_group)
        self.check_commutation_tprms(
            tmo_id=int(self._node_key),
            database=self.database,
            tprm_ids=flat_busy_parameters,
            tmo_collection_name=self.tmo_collection.name,
        )
        self.check_busy_param_uniqueness(
            busy_parameter_groups=self._busy_parameter_groups
        )

    def execute(self):
        node = self.tmo_collection.get({"_key": self._node_key})
        node = DbTmoNode.model_validate(node)
        node.busy_parameter_groups = self._busy_parameter_groups
        try:
            response = self.tmo_collection.update(
                node.model_dump(by_alias=True, mode="json"), return_new=True
            )
        except DocumentRevisionError | DocumentUpdateError:
            raise ValidationError("Cannot update commutation tprms")
        else:
            updated_tmo = TmoNodeResponse.model_validate(response["new"])
            return updated_tmo
