from services.graph import GraphService
from task.models.dto import DbMoEdge, DbMoNode, DbTmoEdge, DbTmoNode
from task.models.enums import Status
from task.models.errors import NotFound
from task.task_abstract import TaskAbstract, TaskChecks


class TmpGtMoInfoTask(TaskAbstract, TaskChecks):
    def __init__(
        self,
        graph_db: GraphService,
        key: str,
        mo_id: int,
    ):
        TaskAbstract.__init__(self, graph_db=graph_db, key=key)
        self.key = key
        self.mo_id = mo_id

    def check(self):
        self.check_status(
            document=self.document, possible_status=[Status.COMPLETE]
        )
        self.check_collection(
            tmo_collection=self.tmo_collection, document=self.document
        )
        self.check_trace_tmo_id(tmo_id=self.trace_tmo_id)

    def find_node_by_mo_id(self, mo_id: int) -> DbMoNode:
        query = """
            FOR doc IN @@mainCollection
                FILTER doc.data.id == @moId
                LIMIT 1
                RETURN doc
        """
        binds = {"@mainCollection": self.main_collection.name, "moId": mo_id}
        response = list(self.database.aql.execute(query=query, bind_vars=binds))
        if not response:
            raise NotFound("Mo id not found")
        db_mo_node = DbMoNode.model_validate(response[0])
        return db_mo_node

    def find_edges(self, node: DbMoNode) -> list[DbMoEdge]:
        query = """
            FOR edge IN @@mainEdgeCollection
                FILTER edge._from == @nodeId OR edge._to == @nodeId
                RETURN edge
        """
        binds = {
            "@mainEdgeCollection": self.main_edge_collection.name,
            "nodeId": node.id,
        }
        response = self.database.aql.execute(query=query, bind_vars=binds)
        db_mo_edges = [DbMoEdge.model_validate(i) for i in response]
        return db_mo_edges

    def connected_with(self, edges: list[DbMoEdge]) -> list[dict]:
        if not edges:
            return []
        node_ids = set()
        for edge in edges:
            node_ids.add(edge.from_)
            node_ids.add(edge.to_)
        query = """
            FOR node IN @@mainCollection
                FILTER node._id IN @nodeIds
                RETURN DISTINCT node
        """
        binds = {
            "@mainCollection": self.main_collection.name,
            "nodeIds": list(node_ids),
        }
        node_names = {
            i["_id"]: i["name"]
            for i in self.database.aql.execute(query=query, bind_vars=binds)
        }
        results = []
        for edge in edges:
            result = {
                "edgeId": edge.id,
                "from": node_names[edge.from_],
                "to": node_names[edge.to_],
            }
            results.append(result)
        return results

    def find_tmo_data(self, node: DbMoNode) -> DbTmoNode:
        query = """
            FOR doc IN @@tmoCollection
                FILTER doc.id == @tmoId
                LIMIT 1
                RETURN doc
        """
        binds = {"@tmoCollection": self.tmo_collection.name, "tmoId": node.tmo}
        response = list(self.database.aql.execute(query=query, bind_vars=binds))
        if not response:
            raise NotFound("Tmo id not found")
        db_tmo_node = DbTmoNode.model_validate(response[0])
        return db_tmo_node

    def find_tmo_edges(self, node: DbTmoNode) -> list[DbTmoEdge]:
        query = """
            FOR edge IN @@tmoEdgeCollection
                FILTER edge._from == @tmoId OR edge._to == @tmoId
                RETURN edge
        """
        binds = {
            "@tmoEdgeCollection": self.tmo_edge_collection.name,
            "tmoId": node.id,
        }
        response = self.database.aql.execute(query=query, bind_vars=binds)
        db_tmo_edges = [DbTmoEdge.model_validate(i) for i in response]
        return db_tmo_edges

    def execute(self):
        mo_node = self.find_node_by_mo_id(mo_id=self.mo_id)
        mo_edge = self.find_edges(node=mo_node)
        connected_nodes = self.connected_with(edges=mo_edge)
        tmo_node = self.find_tmo_data(node=mo_node)
        tmo_edge = self.find_tmo_edges(node=tmo_node)
        return {
            "mo_node": mo_node,
            "mo_edges": mo_edge,
            "connected_nodes": connected_nodes,
            "tmo_node": tmo_node,
            "tmo_edges": tmo_edge,
        }
