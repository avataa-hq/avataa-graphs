from services.graph import GraphService
from task.models.dto import DbMoNode
from task.models.enums import Status
from task.models.outgoing_data import (
    MoNodeResponse,
    NodeTmoResponse,
    TmoResponse,
)
from task.task_abstract import TaskAbstract, TaskChecks


class FindInGraphTask(TaskAbstract, TaskChecks):
    def __init__(
        self,
        graph_db: GraphService,
        key: str,
        find_value: str,
    ):
        TaskAbstract.__init__(self, graph_db=graph_db, key=key)
        self.find_value = find_value

        self.limit = 30
        self.name_coefficient = 10

    def check(self):
        self.check_status(
            document=self.document, possible_status=[Status.COMPLETE]
        )
        self.check_collection(
            document=self.document, tmo_collection=self.tmo_collection
        )
        self.check_view_exists(database=self.database)

    def find(self) -> list[MoNodeResponse]:
        trace_tmo_id = -1
        if self.trace_tmo_data and not self.trace_tmo_data.enabled:
            trace_tmo_id = self.trace_tmo_data.tmo_id
        priority_query = """
            FOR doc IN @@mainCollection
                FILTER doc.name == @value OR doc.label == @value
                FILTER doc.tmo_id != @traceTmoId
                LIMIT @limit
                RETURN doc
        """
        priority_binds = {
            "@mainCollection": self.main_collection.name,
            "value": self.find_value,
            "traceTmoId": trace_tmo_id,
            "limit": self.limit,
        }
        results = []
        exclude_ids = []
        for raw_result in self.database.aql.execute(
            query=priority_query, bind_vars=priority_binds
        ):
            result = MoNodeResponse.model_validate(raw_result)
            results.append(result)
            exclude_ids.append(raw_result["_id"])
        query = """
                LET norm_value = CONCAT("%", TOKENS(SUBSTITUTE(@value, ["_", "%"], ["\\_", "\\%"]), "norm_en")[0], "%")
                FOR doc IN @@searchView
                    SEARCH BOOST(LIKE(doc.name, norm_value), @coefficient)
                        OR LIKE(doc.label, norm_value)
                        OR LIKE(doc.indexed, norm_value)
                    FILTER doc.tmo_id != @traceTmoId
                    FILTER doc._id NOT IN @excludeIds
                    LIMIT @limit
                    SORT bm25(doc) DESC
                    RETURN doc
                """
        limit = self.limit - len(results)
        if limit > 0:
            binds = {
                "@searchView": self.config.search_view,
                "value": self.find_value,
                "coefficient": self.name_coefficient,
                "limit": limit,
                "traceTmoId": trace_tmo_id,
                "excludeIds": exclude_ids,
            }
            for raw_result in self.database.aql.execute(
                query=query, bind_vars=binds
            ):
                result = MoNodeResponse.model_validate(raw_result)
                results.append(result)
        return results

    def get_unique_tmo_data(
        self, nodes: list[MoNodeResponse]
    ) -> list[TmoResponse]:
        if not nodes:
            return []
        unique_tmo = list(set([i.tmo for i in nodes]))
        query = """
                    FOR doc IN @@tmoCollection
                        FILTER doc.id IN @tmos
                        RETURN doc
                """
        binds = {"@tmoCollection": self.tmo_collection.name, "tmos": unique_tmo}
        response = self.database.aql.execute(query=query, bind_vars=binds)
        return [TmoResponse.model_validate(i) for i in response]

    def execute(self) -> NodeTmoResponse:
        nodes = self.find()
        tmos = self.get_unique_tmo_data(nodes=nodes)

        return NodeTmoResponse(nodes=nodes, tmo=tmos)


class GetBreadcrumbsTask(TaskAbstract, TaskChecks):
    def __init__(
        self,
        graph_db: GraphService,
        key: str,
        node_key: str,
    ):
        TaskAbstract.__init__(self, graph_db=graph_db, key=key)
        self.node_key = node_key

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
            main_collection_name=self.main_collection.name,
        )

    def get_parent_node(self, node_key) -> DbMoNode | None:
        query = """
            FOR v, e IN 1..1 OUTBOUND @startNode GRAPH @mainGraph
                FILTER e.connection_type == "p_id"
                LIMIT 1
                FOR doc IN @@mainCollection
                    FILTER doc._id == e._to
                    LIMIT 1
                    RETURN doc
        """
        binds = {
            "startNode": node_key,
            "mainGraph": self.config.graph_data_graph_name,
            "@mainCollection": self.main_collection.name,
        }
        response = list(self.database.aql.execute(query=query, bind_vars=binds))
        if response:
            return DbMoNode.model_validate(response[0])

    def execute(self) -> list[MoNodeResponse]:
        result = []

        node = self.main_collection.get({"_key": self.node_key})
        if node is None:
            return result
        node = DbMoNode.model_validate(node)
        result.append(
            MoNodeResponse.model_validate(node.model_dump(by_alias=True))
        )
        node_id = node.id
        while parent := self.get_parent_node(node_id):
            result.append(
                MoNodeResponse.model_validate(parent.model_dump(by_alias=True))
            )
            node_id = parent.id
        result.reverse()
        return result
