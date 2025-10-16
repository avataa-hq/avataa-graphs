from services.graph import GraphService
from task.helpers.convert_geometry_line import convert_geometry_line
from task.models.dto import DbMoEdge
from task.models.enums import Status
from task.models.outgoing_data import (
    MoEdgeResponse,
    MoNodeResponse,
    NodeEdgeTmoTprmResponse,
    TPRMResponse,
)
from task.task_abstract import TaskAbstract, TaskChecks


class FindEdgesBetweenNodesTask(TaskAbstract, TaskChecks):
    def __init__(
        self,
        graph_db: GraphService,
        key: str,
        node_keys: list[str],
    ):
        TaskAbstract.__init__(self, graph_db=graph_db, key=key)
        self.node_keys = node_keys

    def check(self):
        self.check_status(
            document=self.document, possible_status=[Status.COMPLETE]
        )
        self.check_collection(
            document=self.document, tmo_collection=self.tmo_collection
        )
        self.check_nodes(
            keys=self.node_keys,
            database=self.database,
            main_collection_name=self.main_collection.name,
        )

    def find(self):
        query = """
            FOR edge IN @@mainEdgeCollection
                FILTER edge._from IN @nodeIds
                FILTER edge._to IN @nodeIds
                RETURN edge
        """
        binds = {
            "@mainEdgeCollection": self.main_edge_collection.name,
            "nodeIds": [self.config.get_node_key(i) for i in self.node_keys],
        }
        response = list(self.database.aql.execute(query=query, bind_vars=binds))
        return response

    def get_tprms(self, edges: list[DbMoEdge]) -> list[TPRMResponse]:
        tprm_ids = [i.tprm for i in edges if i.tprm]
        query = """
            FOR doc IN @@tmoCollection
                FOR param in doc.params
                    FILTER param.id IN @tprmIds
                    RETURN param
        """
        binds = {
            "@tmoCollection": self.tmo_collection.name,
            "tprmIds": tprm_ids,
        }
        result = [
            TPRMResponse.model_validate(i)
            for i in self.database.aql.execute(query=query, bind_vars=binds)
        ]
        return result

    def execute(self):
        edges = [DbMoEdge.model_validate(i) for i in self.find()]
        nodes, edges = convert_geometry_line(
            edges=edges,
            main_edge_collection=self.main_edge_collection.name,
            main_collection=self.main_collection.name,
            database=self.database,
        )
        tmos = []
        tprms = self.get_tprms(edges=edges)
        edges = [
            MoEdgeResponse.model_validate(i.model_dump(by_alias=True))
            for i in edges
        ]
        nodes = [
            MoNodeResponse.model_validate(i.model_dump(by_alias=True))
            for i in nodes
        ]
        return NodeEdgeTmoTprmResponse(
            nodes=nodes, edges=edges, tmo=tmos, tprm=tprms
        )
