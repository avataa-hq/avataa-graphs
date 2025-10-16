from __future__ import annotations

from dataclasses import dataclass, field

from task.models.dto import DbMoEdge, DbMoNode
from task.trace_levels_task import ConnectionType, TraceResponse


@dataclass
class NodeChain:
    node: DbMoNode
    edges: list[DbMoEdge] = field(default_factory=list)
    edges_linked_nodes: list[NodeChain] = field(default_factory=list)
    parent_node: NodeChain | None = None
    parent_edge: DbMoEdge | None = None
    children: list[NodeChain] = field(default_factory=list)
    inverted_edges: list[DbMoEdge] = field(default_factory=list)
    inverted_linked_nodes: list[NodeChain] = field(default_factory=list)

    @property
    def id(self) -> str:
        return self.node.id

    @property
    def top_parent_node(self) -> NodeChain:
        if not self.parent_node:
            return self
        return self.parent_node.top_parent_node

    @property
    def top_linked_nodes(self) -> list[NodeChain]:
        result = {}
        for node in self.edges_linked_nodes:
            top_parent_node = node.top_parent_node
            result[top_parent_node.id] = top_parent_node
        for node in self.inverted_linked_nodes:
            top_parent_node = node.top_parent_node
            result[top_parent_node.id] = top_parent_node
        for child in self.children:
            for child_top_link in child.top_linked_nodes:
                result[child_top_link.id] = child_top_link
        return list(result.values())

    @property
    def count_elements(self) -> int:
        count = 1
        for child in self.children:
            count += child.count_elements
        return count

    def get_top_node_chain(
        self, exclude: set[str] | None = None
    ) -> list[NodeChain]:
        if not exclude:
            exclude = set()
        exclude.add(self.id)
        variants: list[list[NodeChain]] = []
        for node in self.top_linked_nodes:
            if node.id in exclude:
                continue
            variant = node.get_top_node_chain(exclude=exclude.copy())
            if not variant:
                continue
            variants.append(variant)
        max_len = 0
        result = []
        for variant in variants:
            if (new_len := len(variant)) > max_len:
                max_len = new_len
                result = variant
        result.insert(0, self)
        return result


class DtoTraceResponse(TraceResponse):
    nodes: list[NodeChain]


class GetSortedGraph:
    def __init__(self, trace: TraceResponse, is_shortest_path: bool = False):
        self.trace = trace
        self.is_shortest_path = is_shortest_path

    def convert_data(self) -> list[NodeChain]:
        nodes = {node.id: NodeChain(node=node) for node in self.trace.nodes}
        for edge in self.trace.edges:
            if edge.connection_type == ConnectionType.P_ID:
                child_node = nodes[edge.from_]
                parent_node = nodes[edge.to_]

                child_node.parent_node = parent_node
                child_node.parent_edge = edge
                parent_node.children.append(child_node)
            else:
                node_from = nodes[edge.from_]
                node_to = nodes[edge.to_]

                node_from.edges.append(edge)
                node_from.edges_linked_nodes.append(node_to)

                node_to.inverted_edges.append(edge)
                node_to.inverted_linked_nodes.append(node_from)
        top_level_nodes = [
            node for node in nodes.values() if node.parent_node is None
        ]
        return top_level_nodes

    @staticmethod
    def leave_largest_cluster(data: list[NodeChain]) -> list[NodeChain]:
        if not data:
            return []
        clusters = []
        queue: dict[str, NodeChain] = {i.id: i for i in data}
        while queue:
            current_key, current_node = queue.popitem()
            links = {
                node.id: [
                    i.id for i in node.top_linked_nodes if i.id != node.id
                ]
                for node in queue.values()
            }
            passed_ids: dict[str, bool] = dict.fromkeys(links.keys(), False)
            cluster: list[NodeChain] = [current_node]
            current_queue: list[str] = [
                i.id
                for i in current_node.top_linked_nodes
                if i.id != current_key
            ]
            while current_queue:
                current_linked_node = current_queue.pop()
                passed_ids[current_linked_node] = True
                cluster.append(queue[current_linked_node])

                for child_current_linked_node in links[current_linked_node]:
                    if (
                        child_current_linked_node in links
                        and not passed_ids[child_current_linked_node]
                    ):
                        current_queue.append(child_current_linked_node)
            clusters.append(cluster)
            queue = {
                node_id: queue[node_id]
                for node_id, passed in passed_ids.items()
                if not passed
            }
        cluster_with_max_len = []
        max_len = 0
        for cluster in clusters:
            cluster_len = len(cluster)
            if cluster_len < max_len:
                continue
            if cluster_len == max_len:
                cluster_with_max_len.append(cluster)
            else:
                max_len = cluster_len
                cluster_with_max_len = [cluster]
        if len(cluster_with_max_len) == 1:
            return cluster_with_max_len[0]
        cluster = cluster_with_max_len[0]
        max_elements = sum(node.count_elements for node in cluster)
        for another_cluster in cluster_with_max_len[1:]:
            another_max_elements = sum(
                node.count_elements for node in another_cluster
            )
            if another_max_elements > max_elements:
                cluster = another_cluster
                max_elements = another_max_elements
        return cluster

    def get_top_level_chain(self, data: list[NodeChain]) -> list[NodeChain]:
        max_elements = 0
        best_node: NodeChain = data[0]
        for node in data:
            node_elements = node.count_elements
            if node_elements <= max_elements:
                continue
            max_elements = node_elements
            best_node = node
        right_way = best_node.get_top_node_chain()
        left_way = best_node.get_top_node_chain(
            exclude=set(i.id for i in right_way)
        )[:0:-1]
        way = left_way + right_way
        return way

    def get_expanded_chain(self, data: list[NodeChain]) -> TraceResponse:
        def get_way(
            node: NodeChain, to_node: NodeChain
        ) -> DtoTraceResponse | None:
            to_node_id = to_node.id
            top_linked_node_ids = [i.id for i in node.top_linked_nodes]
            if to_node.top_parent_node.id not in top_linked_node_ids:
                return
            children_results = []
            for child in node.children:
                child_result = get_way(node=child, to_node=to_node)
                if not child_result:
                    continue
                children_results.append(child_result)
            if not children_results:
                for linked_node, linked_edge in zip(
                    node.edges_linked_nodes, node.edges
                ):  # type: NodeChain, DbMoEdge
                    top_linked_node_id = linked_node.top_parent_node.id
                    if to_node_id == top_linked_node_id:
                        edges_list = [linked_edge]
                        if node.parent_node:
                            edges_list.append(node.parent_edge)
                        return DtoTraceResponse(nodes=[node], edges=edges_list)
                for linked_node, linked_edge in zip(
                    node.inverted_linked_nodes, node.inverted_edges
                ):  # type: NodeChain, DbMoEdge
                    top_linked_node_id = linked_node.top_parent_node.id
                    if to_node_id == top_linked_node_id:
                        edges_list = [linked_edge]
                        if node.parent_node:
                            edges_list.append(node.parent_edge)
                        return DtoTraceResponse(nodes=[node], edges=edges_list)

            max_nodes_len = 0
            result = None
            for children_result in children_results:
                result_len = len(children_result.nodes) + len(
                    children_result.edges
                )
                if result_len > max_nodes_len:
                    max_nodes_len = result_len
                    result = children_result
            if result:
                result.nodes.append(node)
                if node.parent_edge:
                    result.edges.append(node.parent_edge)
            return result

        nodes: list[DbMoNode] = []
        edges: list[DbMoEdge] = []
        for i in range(len(data) - 1):
            current_node = data[i]
            next_node = data[i + 1]

            left_way = get_way(node=current_node, to_node=next_node)
            new_current_node = left_way.nodes[0]
            right_way = get_way(node=next_node, to_node=new_current_node)
            if not right_way:
                right_way = get_way(node=next_node, to_node=current_node)
                new_current_node = right_way.nodes[0]
                new_left_way = get_way(
                    node=current_node, to_node=new_current_node
                )
                if new_left_way:
                    left_way = new_left_way

            if nodes:
                left_way.nodes.pop()
                right_way.edges.pop(0)
            nodes.extend(reversed([i.node for i in left_way.nodes]))
            edges.extend(reversed(left_way.edges))

            nodes.extend([i.node for i in right_way.nodes])
            edges.extend(right_way.edges)
        return TraceResponse(nodes=nodes, edges=edges)

    def execute(self):
        if not self.trace.nodes or not self.trace.edges:
            return TraceResponse(nodes=[], edges=[])
        data = self.convert_data()
        if not self.is_shortest_path:
            data = self.leave_largest_cluster(data=data)
            chain = self.get_top_level_chain(data)
        else:
            chain = data
        trace = self.get_expanded_chain(data=chain)
        return trace
