from collections import Counter
import json
from typing import Iterator

from pydantic import BaseModel

from services.graph import GraphService
from task.models.dto import DbMoEdge, DbMoNode, DbTmoNode
from task.models.enums import ConnectionType, Status
from task.models.outgoing_data import MoNodeResponse, NodeEdgeResponse
from task.task_abstract import TaskAbstract, TaskChecks
from task.trace_levels_task import TraceResponse, Tracker, TrackingType
from task.tracking_task import GetSortedGraph


class DtoDataResponse(BaseModel):
    nodes: list[DbMoNode]
    edges: list[DbMoEdge]
    tmos: list[DbTmoNode]


def _get_straight_way(trace: TraceResponse):
    # nodes
    node_ids = [node.id for node in trace.nodes]
    counts = Counter(node_ids)
    new_nodes = trace.nodes
    for key, value in counts.items():
        if value <= 1:
            continue
        try:
            first_entry = node_ids.index(key)
            last_entry = len(node_ids) - 1 - node_ids[::-1].index(key)
            if first_entry >= last_entry:
                continue
            node_ids = node_ids[:first_entry] + node_ids[last_entry:]
            new_nodes = new_nodes[:first_entry] + new_nodes[last_entry:]
        except ValueError:
            continue

    # edges
    node_ids_set = set(node_ids)
    new_edges = []
    for edge in trace.edges:
        if edge.to_ in node_ids_set and edge.from_ in node_ids_set:
            new_edges.append(edge)

    return TraceResponse(nodes=new_nodes, edges=new_edges)


class GetAllPathsForNodeTask(TaskAbstract, TaskChecks):
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
            tmo_collection=self.tmo_collection, document=self.document
        )
        self.check_trace_tmo_id(tmo_id=self.trace_tmo_id)
        self.check_nodes(
            keys=[self.node_key],
            main_collection_name=self.main_collection.name,
            database=self.database,
        )

    def rename_traces(self, traces: list[MoNodeResponse]):
        for trace in traces:
            for param in trace.data.params:
                if param.tprm_id == self.trace_tprm_id:
                    trace.name = (
                        param.value
                        if isinstance(param.value, str)
                        else json.dumps(param.value, default=str)
                    )
                    break

    def execute(self) -> list[MoNodeResponse]:
        # check is trace
        query = """
            FOR doc IN @@mainCollection
                FILTER doc._id == @nodeId
                FILTER doc.tmo == @tmoId
                LIMIT 1
                RETURN doc
        """
        binds = {
            "@mainCollection": self.main_collection.name,
            "nodeId": self.config.get_node_key(self.node_key),
            "tmoId": self.trace_tmo_id,
        }
        response = list(self.database.aql.execute(query=query, bind_vars=binds))
        if not response:
            # if not trace
            query = """
                FOR v IN 1 OUTBOUND @nodeId GRAPH @mainGraph
                    FILTER v.tmo == @tmoId
                    RETURN DISTINCT v
            """
            binds = {
                "mainGraph": self.config.graph_data_graph_name,
                "nodeId": self.config.get_node_key(self.node_key),
                "tmoId": self.trace_tmo_id,
            }
            response = self.database.aql.execute(query=query, bind_vars=binds)
        result = [MoNodeResponse.model_validate(i) for i in response]
        if self.trace_tprm_id:
            self.rename_traces(traces=result)
        return result


class GetPathTask(TaskAbstract, TaskChecks):
    def __init__(
        self,
        graph_db: GraphService,
        key: str,
        trace_node_key: str,
        level: TrackingType,
    ):
        TaskAbstract.__init__(self, graph_db=graph_db, key=key)
        self.key = key
        self.trace_node_key = trace_node_key
        self.level = level
        self.node_id = (
            f"{self.config.graph_data_collection_name}/{self.trace_node_key}"
        )

    def check(self):
        self.check_status(
            document=self.document, possible_status=[Status.COMPLETE]
        )
        self.check_collection(
            tmo_collection=self.tmo_collection, document=self.document
        )
        self.check_trace_tmo_id(tmo_id=self.trace_tmo_id)

    def get_data(self):
        query = f"""
            LET nodes = (FOR v, e IN 1 INBOUND @nodeId GRAPH @mainGraph
                FILTER e.connection_type IN ["{ConnectionType.MO_LINK.value}", "{ConnectionType.TWO_WAY_MO_LINK.value}"]
                RETURN DISTINCT v)
            LET node_ids = (
                FOR node IN nodes
                    RETURN node._id)
            LET edges = (
                FOR edge IN @@edgeCollection
                    FILTER edge._to IN node_ids
                    FILTER edge._from IN node_ids
                    FILTER edge.connection_type != "geometry_line"
                    RETURN DISTINCT edge
                    )
            RETURN {{"nodes": nodes, "edges": edges}}
        """
        binds = {
            "nodeId": self.node_id,
            "mainGraph": self.config.graph_data_graph_name,
            "@edgeCollection": self.config.graph_data_edge_name,
        }
        results = list(self.database.aql.execute(query=query, bind_vars=binds))
        result = results[0]
        nodes = result["nodes"]
        edges = result["edges"]

        tmos = list(self.tmo_collection.all())
        return DtoDataResponse.model_validate(
            dict(nodes=nodes, edges=edges, tmos=tmos)
        )

    def delete_end_lines(self, trace: NodeEdgeResponse) -> NodeEdgeResponse:
        if not trace.nodes:
            return trace

        nodes_to_delete = set()
        first_node = trace.nodes[0]
        last_node = trace.nodes[-1]
        line_tmo_ids = set(
            i.tmo_id for i in trace.tmo if i.geometry_type == "line"
        )
        if first_node.tmo in line_tmo_ids:
            nodes_to_delete.add(first_node.key)
            trace.nodes = trace.nodes[1:]
        if last_node != first_node and last_node.tmo in line_tmo_ids:
            nodes_to_delete.add(last_node.key)
            trace.nodes = trace.nodes[:-1]
        if not nodes_to_delete:
            return trace

        new_edges = []
        for edge in trace.edges:
            if edge.source in nodes_to_delete:
                continue
            elif edge.target in nodes_to_delete:
                continue
            new_edges.append(edge)
        trace.edges = new_edges
        return self.delete_end_lines(trace=trace)

    def execute(self):
        data = self.get_data()
        traker = Tracker(
            nodes=data.nodes,
            edges=data.edges,
            tmos=data.tmos,
            expand_lonely_node=True,
        )
        trace = traker.get_trace(tracking_type=self.level)

        if self.level != TrackingType.GRAPH:
            trace_builder = GetSortedGraph(trace=trace)
            trace = trace_builder.execute()

        if self.level == TrackingType.STRAIGHT:
            trace = _get_straight_way(trace)

        unique_tmo = list(set([i.tmo for i in trace.nodes]))
        tmo = [
            tmo.model_dump(mode="json", by_alias=True)
            for tmo in self._get_tmos_data(tmo_ids=unique_tmo)
        ]
        result_dict = {"tmo": tmo}
        result_dict.update(trace.model_dump(by_alias=True))
        result = NodeEdgeResponse.model_validate(result_dict)
        if self.level != TrackingType.GRAPH:
            result = self.delete_end_lines(trace=result)

        return result


class FindCommonPath(TaskAbstract, TaskChecks):
    def __init__(
        self,
        graph_db: GraphService,
        key: str,
        trace_node_a_key: str,
        trace_node_b_key: str,
        level: TrackingType,
    ):
        TaskAbstract.__init__(self, graph_db=graph_db, key=key)
        self.key = key
        self.trace_node_a_key = trace_node_a_key
        self.trace_node_b_key = trace_node_b_key
        self.level = level
        self.node_a_id = (
            f"{self.config.graph_data_collection_name}/{self.trace_node_a_key}"
        )
        self.node_b_id = (
            f"{self.config.graph_data_collection_name}/{self.trace_node_b_key}"
        )

    def check(self):
        self.check_status(
            document=self.document, possible_status=[Status.COMPLETE]
        )
        self.check_collection(
            tmo_collection=self.tmo_collection, document=self.document
        )
        self.check_trace_tmo_id(tmo_id=self.trace_tmo_id)
        self.check_nodes(
            keys=[self.trace_node_a_key, self.trace_node_b_key],
            main_collection_name=self.main_collection.name,
            database=self.database,
        )

    def get_trace_nodes_ids(self, trace_node_key: str) -> list[str]:
        query = """
             FOR v, e IN 1 INBOUND @nodeId GRAPH @mainGraph
                FILTER e.connection_type == "mo_link"
                RETURN DISTINCT v._id
         """
        binds = {
            "nodeId": self.config.get_node_key(trace_node_key),
            "mainGraph": self.config.graph_data_graph_name,
        }
        results = list(self.database.aql.execute(query=query, bind_vars=binds))
        return results

    def get_nodes_by_ids(self, node_ids: list[str]) -> list[DbMoNode]:
        query = """
            FOR doc IN @@mainCollection
                FILTER doc._id IN @nodeIds
                RETURN doc
        """
        binds = {
            "@mainCollection": self.main_collection.name,
            "nodeIds": node_ids,
        }
        results = [
            DbMoNode.model_validate(node)
            for node in self.database.aql.execute(query=query, bind_vars=binds)
        ]
        return results

    def get_edges_by_ids(self, node_ids: list[str]) -> list[DbMoEdge]:
        query = """
            FOR edge IN @@mainEdgeCollection
                FILTER edge._to IN @nodeIds
                FILTER edge._from IN @nodeIds
                FILTER edge.connection_type != "geometry_line"
                RETURN edge
        """
        binds = {
            "@mainEdgeCollection": self.main_edge_collection.name,
            "nodeIds": node_ids,
        }
        results = [
            DbMoEdge.model_validate(edge)
            for edge in self.database.aql.execute(query=query, bind_vars=binds)
        ]
        return results

    @staticmethod
    def filter_path_by_common_nodes(
        trace: TraceResponse, common_node_ids: list[str]
    ):
        def trace_iterator() -> Iterator[TraceResponse]:
            _trace = TraceResponse(nodes=[], edges=[])
            for _node in trace.nodes:
                if _node.id in common_node_ids:
                    _trace.nodes.append(_node)
                    continue
                if _trace.nodes:
                    _node_ids = {i.id for i in _trace.nodes}
                    for _edge in trace.edges:
                        if _edge.from_ in _node_ids and _edge.to_ in _node_ids:
                            _trace.edges.append(_edge)
                    yield _trace
                _trace = TraceResponse(nodes=[], edges=[])
            if _trace.nodes:
                _node_ids = {i.id for i in _trace.nodes}
                for _edge in trace.edges:
                    if _edge.from_ in _node_ids and _edge.to_ in _node_ids:
                        _trace.edges.append(_edge)
            yield _trace

        longest_trace = None
        for current_trace in trace_iterator():
            if longest_trace is None:
                longest_trace = current_trace
                continue
            if len(current_trace.nodes) > len(longest_trace.nodes):
                longest_trace = current_trace
        return longest_trace

    def execute(self):
        trace_a_ids = set(self.get_trace_nodes_ids(self.trace_node_a_key))
        trace_b_ids = set(self.get_trace_nodes_ids(self.trace_node_b_key))
        shortest_ids = (
            trace_b_ids if len(trace_a_ids) < len(trace_b_ids) else trace_b_ids
        )
        intersection = list(trace_a_ids.intersection(trace_b_ids))
        nodes = self.get_nodes_by_ids(node_ids=list(shortest_ids))
        edges = self.get_edges_by_ids(node_ids=list(shortest_ids))
        tmo_ids = set(i.tmo for i in nodes)
        tmos = self._get_tmos_data(tmo_ids=list(tmo_ids))

        traker = Tracker(
            nodes=nodes, edges=edges, tmos=tmos, expand_lonely_node=True
        )
        trace = traker.get_trace(tracking_type=self.level)

        if self.level != TrackingType.GRAPH:
            trace_builder = GetSortedGraph(trace=trace)
            trace = trace_builder.execute()

        if self.level == TrackingType.STRAIGHT:
            trace = _get_straight_way(trace)

        trace = self.filter_path_by_common_nodes(
            trace=trace, common_node_ids=intersection
        )
        unique_tmo = list(set([i.tmo for i in trace.nodes]))
        tmo = [
            tmo.model_dump(mode="json", by_alias=True)
            for tmo in self._get_tmos_data(tmo_ids=unique_tmo)
        ]
        result_dict = {"tmo": tmo}
        result_dict.update(trace.model_dump(by_alias=True))
        result = NodeEdgeResponse.model_validate(result_dict)
        return result
