from __future__ import annotations

from arango import AQLQueryExecuteError
from pydantic import BaseModel, Field, computed_field

from config import PathFinderConfig
from services.graph import GraphService
from task.models.dto import DbMoEdge, DbMoNode, DbTmoNode, Path
from task.models.enums import ConnectionType, Status, TrackingType
from task.models.errors import TimeOutError
from task.models.outgoing_data import PathResponse
from task.task_abstract import TaskAbstract, TaskChecks
from task.trace_levels_task import Tracker
from task.trace_tasks import _get_straight_way
from task.tracking_task import GetSortedGraph


class PathFinderNode(BaseModel):
    node: DbMoNode
    edges: list[DbMoEdge] = Field(default_factory=list)
    parent: PathFinderNode | None = None
    children: list[PathFinderNode] = Field(default_factory=list)

    @computed_field
    @property
    def length(self) -> int:
        length = 1
        for child in self.children:
            length += child.length
        return length


class FindPathBetweenNodesTask(TaskAbstract, TaskChecks):
    def __init__(
        self,
        graph_db: GraphService,
        key: str,
        node_key_a: str,
        node_key_b: str,
        level: TrackingType,
    ):
        TaskAbstract.__init__(self, graph_db=graph_db, key=key)
        self.node_key_a = node_key_a
        self.node_key_b = node_key_b
        self.level = level

        config = PathFinderConfig()
        self.response_limit: int = config.response_limit
        self.search_limit: int = config.search_limit

    def check(self):
        self.check_status(
            document=self.document, possible_status=[Status.COMPLETE]
        )
        self.check_collection(
            document=self.document, tmo_collection=self.tmo_collection
        )
        self.check_nodes(
            keys=[self.node_key_a, self.node_key_b],
            main_collection_name=self.main_collection.name,
            database=self.database,
        )
        self.check_view_exists(database=self.database)

    def replace_with_real_edges(self, raw_data: list[dict]):
        query = """
            FOR connection in @connections
                LET connectionEdge = (FOR edge IN @@mainEdgeCollection
                        FILTER edge.virtual == false
                        FILTER edge.is_trace == false
                        FILTER (edge._from == connection._from AND edge._to == connection._to)
                            OR (edge._to == connection._from AND edge._from == connection._to)
                        SORT edge.connection_type DESC
                        LIMIT 1
                        RETURN edge)
                RETURN FIRST(connectionEdge)
        """
        binds = {"@mainEdgeCollection": self.main_edge_collection.name}
        for shortest_path in raw_data:
            path_edges = shortest_path["edges"]
            if not path_edges:
                continue
            binds["connections"] = path_edges
            response = list(
                self.database.aql.execute(query=query, bind_vars=binds)
            )
            shortest_path["edges"] = response

    def find(self) -> list[Path]:
        query = """
            FOR p IN ANY K_SHORTEST_PATHS @point_a TO @point_b
                GRAPH @pathGraph
                LIMIT @limit
                RETURN p
        """
        binds = {
            "point_a": self.config.get_node_key(self.node_key_a),
            "point_b": self.config.get_node_key(self.node_key_b),
            "pathGraph": self.config.graph_data_path_graph_name,
            "limit": self.search_limit,
        }
        try:
            response = self.database.aql.execute(query=query, bind_vars=binds)
        except AQLQueryExecuteError:
            try:
                binds["limit"] = 1
                response = self.database.aql.execute(
                    query=query, bind_vars=binds
                )
            except AQLQueryExecuteError:
                raise TimeOutError(
                    "The request could not be completed within the allotted time. "
                    "Most likely there is no connection between the elements"
                )
        response = list(response)
        self.replace_with_real_edges(response)
        return [Path.model_validate(i) for i in response]

    def collapse_path(self, path: Path) -> list[PathFinderNode]:
        nodes_dict = {
            node.id: PathFinderNode(
                node=node.model_dump(mode="json", by_alias=True)
            )
            for node in path.nodes
        }
        for edge in path.edges:  # type: DbMoEdge
            if edge.from_ not in nodes_dict or edge.to_ not in nodes_dict:
                continue
            nodes_dict[edge.from_].edges.append(edge)
            if edge.connection_type == ConnectionType.P_ID.value:
                child = nodes_dict[edge.from_]
                parent = nodes_dict[edge.to_]
                child.parent = parent
                parent.children.append(child)
        parents = [
            path_node
            for path_node in nodes_dict.values()
            if not path_node.parent
        ]
        return parents

    def exclude_identical_paths(self, paths: list[Path]) -> list[Path]:
        paths_by_hash: dict[int, Path] = {}
        for path in paths:
            collapsed_path = self.collapse_path(path=path)
            path_parent_point_ids = []
            tmos_dict = {tmo.tmo_id: tmo for tmo in path.tmo}
            for path_item in collapsed_path:
                tmo = tmos_dict[path_item.node.tmo]
                if tmo.geometry_type != "line":
                    path_parent_point_ids.append(path_item.node.key)
            path_hash = hash(frozenset(path_parent_point_ids))
            if path_hash not in paths_by_hash:
                paths_by_hash[path_hash] = path
            elif paths_by_hash[path_hash].length > path.length:
                paths_by_hash[path_hash] = path
        # sort by length
        sorted_paths = sorted(paths_by_hash.values(), key=lambda x: x.length)
        return sorted_paths[: self.response_limit]

    def get_all_edges_between_nodes(
        self, nodes: list[DbMoNode]
    ) -> list[DbMoEdge]:
        if not nodes:
            return []
        node_ids = [i.id for i in nodes]
        query = """
            FOR edge in @@mainEdgeCollection
                FILTER edge._from IN @nodeIds
                FILTER edge._to IN @nodeIds
                FILTER edge.connection_type != "geometry_line"
                RETURN edge
        """
        binds = {
            "nodeIds": node_ids,
            "@mainEdgeCollection": self.main_edge_collection.name,
        }
        response = self.database.aql.execute(query=query, bind_vars=binds)
        edges = [DbMoEdge.model_validate(i) for i in response]
        return edges

    def execute(self) -> list[PathResponse]:
        patches = self.find()
        for path in patches:
            tmos = self._get_tmos_data(
                tmo_ids=list(set(map(lambda x: x.tmo, path.nodes)))
            )
            tmos = [
                DbTmoNode.model_validate(
                    tmo.model_dump(by_alias=True, mode="json")
                )
                for tmo in tmos
            ]
            path.tmo = tmos
        patches = self.exclude_identical_paths(paths=patches)
        results = []
        for path in patches:
            edges = self.get_all_edges_between_nodes(nodes=path.nodes)
            traker = Tracker(nodes=path.nodes, edges=edges, tmos=path.tmo)
            trace = traker.get_trace(tracking_type=self.level)

            if self.level != TrackingType.GRAPH:
                trace_builder = GetSortedGraph(
                    trace=trace, is_shortest_path=True
                )
                trace = trace_builder.execute()

            if self.level == TrackingType.STRAIGHT:
                trace = _get_straight_way(trace)
            trace_dict = trace.model_dump(by_alias=True, mode="json")
            trace_dict["weight"] = len(trace.edges)
            trace_dict["tmo"] = [
                i.model_dump(by_alias=True, mode="json") for i in path.tmo
            ]
            result = PathResponse.model_validate(trace_dict)
            results.append(result)
        return results
