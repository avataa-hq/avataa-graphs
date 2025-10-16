from __future__ import annotations

from pydantic import BaseModel

from task.models.dto import DbMoEdge, DbMoNode, DbTmoNode
from task.models.enums import ConnectionType, TrackingType


class TraceResponse(BaseModel):
    nodes: list[DbMoNode]
    edges: list[DbMoEdge]

    def extend(self, trace_response: TraceResponse):
        self.nodes.extend(trace_response.nodes)
        self.edges.extend(trace_response.edges)
        return self

    def drop_orphan_edges(self):
        node_ids = set([node.id for node in self.nodes])
        self.edges = [edge for edge in self.edges if edge.to_ in node_ids]
        return self

    def filter_same_edges(self):
        edges = []
        edges_passed = set()
        for edge in self.edges:
            if edge.id in edges_passed:
                continue
            edges.append(edge)
            edges_passed.add(edge.id)
        self.edges = edges
        return self


class TrackNode:
    def __init__(self, node: DbMoNode, is_global: bool):
        if not isinstance(node, DbMoNode):
            raise ValueError(f"Incorrect type of {type(node)}")
        self.node: DbMoNode = node
        self.is_global: bool = is_global
        self._parent_link: DbMoEdge | None = None
        self._parent_node: TrackNode | None = None
        self._real_link: list[DbMoEdge] = []
        self._virtual_link: list[DbMoEdge] = []
        self._children: list[TrackNode] = []

    @property
    def parent_id(self) -> str | None:
        if not self._parent_node:
            return None
        return self._parent_node.node.id

    def add_link(self, link: DbMoEdge):
        if link.connection_type == ConnectionType.P_ID:
            self._parent_link = link
        elif link.virtual:
            self._virtual_link.append(link)
        else:
            self._real_link.append(link)

    def set_parent_node(self, parent: TrackNode):
        self._parent_node = parent

    def add_child(self, child: TrackNode):
        child.set_parent_node(parent=self)
        self._children.append(child)

    def _get_self_trace(self) -> TraceResponse:
        result = TraceResponse(nodes=[], edges=[])
        result.nodes.append(self.node)
        result.edges.extend(self._real_link)
        if self._parent_link:
            result.edges.append(self._parent_link)
        return result

    def _convert_self_links_to_parent_links(
        self, nodes_by_id: dict[str, TrackNode]
    ):
        results = []
        if not self._parent_node:
            return results
        for link in [*self._real_link, *self._virtual_link]:  # type: DbMoEdge
            to_node = nodes_by_id.get(link.to_)
            if not to_node:
                continue
            to_parent_id = to_node.get_nearest_global_parent()
            if not to_parent_id:
                continue
            new_link = link.model_copy(deep=True)
            new_link.virtual = True
            new_link.from_ = self.parent_id
            new_link.to_ = to_parent_id
            results.append(new_link)
        return results

    def get_trace(
        self, tracking_type: TrackingType, nodes_by_id: dict[str, TrackNode]
    ) -> TraceResponse | None:
        result = None
        if not self.node.grouped_by_tprm:
            match tracking_type:
                case TrackingType.FULL:
                    result = self._get_self_trace()
                    result.edges.extend(self._virtual_link)
                case TrackingType.LOCAL:
                    if not self.is_global:
                        edges = self._convert_self_links_to_parent_links(
                            nodes_by_id=nodes_by_id
                        )
                        return TraceResponse(edges=edges, nodes=[])
                    result = self._get_self_trace()
                    for child in self._children:
                        child_response = child.get_trace(
                            tracking_type, nodes_by_id=nodes_by_id
                        )
                        if child_response.nodes:
                            result.extend(child_response)
                        else:
                            to_set = set([i.to_ for i in child_response.edges])
                            edges = []
                            for virtual_link in self._virtual_link:
                                if virtual_link.to_ in to_set:
                                    edges.append(virtual_link)
                            result.edges.extend(edges)
                case TrackingType.NONE:
                    result = self._get_self_trace()
                    for child in self._children:
                        child_response = child.get_trace(
                            tracking_type, nodes_by_id=nodes_by_id
                        )
                        if child_response:
                            result.extend(child_response)
                    if not self._children:
                        result.edges.extend(self._virtual_link)
                case TrackingType.STRAIGHT:
                    result = self._get_self_trace()
                    for child in self._children:
                        child_response = child.get_trace(
                            tracking_type, nodes_by_id=nodes_by_id
                        )
                        if child_response:
                            result.extend(child_response)
                    if not self._children:
                        result.edges.extend(self._virtual_link)
                case TrackingType.GRAPH:
                    result = self._get_self_trace()
                    for child in self._children:
                        child_response = child.get_trace(
                            tracking_type, nodes_by_id=nodes_by_id
                        )
                        if child_response:
                            result.extend(child_response)
                    # if not self._children:
                    #     result.edges.extend(self._virtual_link)
        else:
            # exclude virtual levels
            new_trace = TraceResponse(nodes=[], edges=[])
            for child in self._children:
                child_response = child.get_trace(
                    tracking_type, nodes_by_id=nodes_by_id
                )
                if not child_response:
                    continue
                if self._parent_node:
                    for edge in child_response.edges:
                        if edge.to_ == self.node.id:
                            edge.to_ = self._parent_node.node.id
                else:
                    edges = []
                    for edge in child_response.edges:
                        if edge.to_ != self.node.id:
                            edges.append(edge)
                    child_response.edges = edges
                new_trace.extend(child_response)
            if len(new_trace.edges) > 0 or len(new_trace.nodes) > 0:
                result = new_trace
        return result

    def get_nearest_global_parent(self):
        if self.is_global or not self._parent_node:
            return self.node.id
        else:
            return self._parent_node.get_nearest_global_parent()


class Tracker:
    def __init__(
        self,
        nodes: list[DbMoNode],
        edges: list[DbMoEdge],
        tmos: list[DbTmoNode],
        expand_lonely_node: bool = False,
    ):
        self.nodes = nodes
        self.edges = edges
        self.expand_lonely_node = expand_lonely_node

        self.tmos_dict: dict[int, DbTmoNode] = {i.tmo_id: i for i in tmos}
        self.nodes_by_id: dict[str, TrackNode] = {}
        self.rebuild(nodes=nodes, edges=edges)
        self._top_level = [
            i for i in self.nodes_by_id.values() if not i.parent_id
        ]

    def rebuild(self, nodes: list[DbMoNode], edges: list[DbMoEdge]) -> bool:
        nodes_by_id: dict[str, TrackNode] = {
            i.id: TrackNode(
                node=i, is_global=self.tmos_dict[i.tmo].global_uniqueness
            )
            for i in nodes
        }
        for edge in edges:
            node = nodes_by_id[edge.from_]
            node.add_link(edge)
            if edge.connection_type == ConnectionType.P_ID:
                parent_node = nodes_by_id[edge.to_]
                parent_node.add_child(child=node)
        if self.expand_lonely_node:
            top_level_not_line = []
            for node in nodes_by_id.values():
                if node.parent_id:
                    continue
                tmo = self.tmos_dict[node.node.tmo]
                if tmo.geometry_type == "line":
                    continue
                top_level_not_line.append(node)
            if len(top_level_not_line) == 0:
                return False
            if len(top_level_not_line) == 1:
                node = top_level_not_line[0]
                new_nodes = [i for i in nodes if i != node]
                new_edges = [
                    i
                    for i in edges
                    if i.from_ != node.node.id and i.to_ != node.node.id
                ]
                response = self.rebuild(nodes=new_nodes, edges=new_edges)
                if response:
                    return True
                self.nodes = nodes
                self.edges = edges
                self.nodes_by_id = nodes_by_id
                return False
            else:
                self.nodes = nodes
                self.edges = edges
                self.nodes_by_id = nodes_by_id
                return True
        else:
            self.nodes = nodes
            self.edges = edges
            self.nodes_by_id = nodes_by_id
            return True

    def get_trace(self, tracking_type: TrackingType) -> TraceResponse:
        result = TraceResponse(nodes=[], edges=[])
        for track_node in self._top_level:
            response = track_node.get_trace(
                tracking_type=tracking_type, nodes_by_id=self.nodes_by_id
            )
            if response:
                result.extend(response)
        if tracking_type != TrackingType.GRAPH:
            result.drop_orphan_edges()
        return result
