from collections import defaultdict

from services.graph import GraphService
from task.helpers.convert_geometry_line import convert_geometry_line
from task.models.dto import DbMoEdge, DbMoNode
from task.models.enums import ConnectionType, Status
from task.models.errors import NotFound
from task.models.outgoing_data import (
    CollapseNodeResponse,
    CommutationResponse,
    MoEdgeResponse,
    MoNodeResponse,
    NodeEdgeCommutationResponse,
    NodeEdgeTmoTprmResponse,
    TmoResponse,
    TPRMResponse,
)
from task.task_abstract import TaskAbstract, TaskChecks, TaskWithMaxSize


class GetTopLevelAnalysisTask(TaskAbstract, TaskWithMaxSize, TaskChecks):
    def __init__(
        self,
        graph_db: GraphService,
        key: str,
        max_size: int = 0,
    ):
        TaskAbstract.__init__(self, graph_db=graph_db, key=key)
        TaskWithMaxSize.__init__(self, response_length=max_size)

    def check(self):
        self.check_status(
            document=self.document, possible_status=[Status.COMPLETE]
        )
        self.check_collection(
            document=self.document, tmo_collection=self.tmo_collection
        )

    def _get_nodes_edges(self) -> NodeEdgeCommutationResponse:
        start_from = self.config_collection.get({"_key": "start_from"})
        tmo_id = (
            start_from.get("tmo_id", self.document.tmo_id)
            if start_from
            else self.document.tmo_id
        )
        tmo_data = self._get_tmos_data(tmo_ids=[tmo_id])
        tprm_id = start_from.get("tprm_id", None) if start_from else None
        query = """
            LET nodes = (
                FOR doc IN @@mainCollection
                    FILTER doc.tmo == @tmoId
                    FILTER doc.grouped_by_tprm == @tprmId
                    RETURN doc
                )

            LET nodeIds = (
                FOR doc IN nodes
                    RETURN doc._id
                )

            LET edges = (
                FOR edge IN @@edgeCollection
                    FILTER edge._from IN nodeIds
                    FILTER edge._to IN nodeIds
                    RETURN edge
                )

            RETURN {"nodes": nodes, "edges": edges}
        """
        binds = {
            "tmoId": tmo_id,
            "tprmId": tprm_id,
            "@mainCollection": self.main_collection.name,
            "@edgeCollection": self.main_edge_collection.name,
        }
        response = next(self.database.aql.execute(query=query, bind_vars=binds))
        response["tmo"] = [
            tmo.model_dump(by_alias=True, mode="json") for tmo in tmo_data
        ]
        response = NodeEdgeCommutationResponse.model_validate(response)
        return response

    def execute(self):
        response = self._get_nodes_edges()
        response = self.check_response_length(response=response)
        return response


class ExpandNodesTask(TaskAbstract, TaskWithMaxSize, TaskChecks):
    def __init__(
        self,
        graph_db: GraphService,
        key: str,
        node_key: str,
        neighboring_node_keys: list[str],
        expand_edges: bool,
        max_size: int = 0,
        return_commutation_label: bool = False,
    ):
        TaskAbstract.__init__(self, graph_db=graph_db, key=key)
        TaskWithMaxSize.__init__(self, response_length=max_size)

        self.node_key = node_key
        self.commutation_label = "label" if return_commutation_label else "name"
        self.neighboring_node_keys = list(
            set(neighboring_node_keys).difference([node_key])
        )
        self.expand_edges = expand_edges

    def check(self):
        self.check_status(
            document=self.document, possible_status=[Status.COMPLETE]
        )
        self.check_collection(
            document=self.document, tmo_collection=self.tmo_collection
        )
        self.check_nodes(
            keys=[*self.neighboring_node_keys, self.node_key],
            database=self.database,
            main_collection_name=self.main_collection.name,
        )

    def _get_child_nodes(self, node_key: str):
        query = """
            FOR v,e,p IN 1..1 INBOUND @nodeKey GRAPH @mainGraph
                FILTER e.connection_type == "p_id"
                RETURN v
        """
        binds = {
            "nodeKey": node_key,
            "mainGraph": self.config.graph_data_graph_name,
        }
        response = list(self.database.aql.execute(query=query, bind_vars=binds))
        return response

    def _get_children_links(
        self, children_nodes: list[str], neighbour_nodes: list[str]
    ) -> list[dict]:
        query = """
            FOR doc IN @@mainEdge
                FILTER (doc._from IN @first AND doc._to IN @second) OR (doc._from IN @second AND doc._to IN @first)
                RETURN doc
                """
        children_nodes = [self.config.get_node_key(i) for i in children_nodes]
        neighbour_nodes = [self.config.get_node_key(i) for i in neighbour_nodes]
        neighbour_nodes.extend(children_nodes)
        binds = {
            "first": children_nodes,
            "second": neighbour_nodes,
            "@mainEdge": self.config.graph_data_edge_name,
        }
        results = list(self.database.aql.execute(query=query, bind_vars=binds))
        return results

    def group_as_params(
        self,
    ) -> list[dict]:
        results = []
        node = self.main_collection.get(document={"_key": self.node_key})
        tmo_item = self.tmo_collection.get(document={"_key": str(node["tmo"])})
        if not tmo_item["global_uniqueness"]:
            return results
        query = """
            FOR v, e, p IN 1 INBOUND @nodeId GRAPH @tmoGraph
                FILTER e.link_type == "p_id"
                FILTER v.global_uniqueness == false
                FILTER v.show_as_a_table != false
                RETURN v
        """
        binds = {
            "nodeId": tmo_item["_id"],
            "tmoGraph": self.config.tmo_graph_name,
        }
        results = list(self.database.aql.execute(query=query, bind_vars=binds))
        return results

    def replace_with_expanded_edges(
        self, response: NodeEdgeCommutationResponse
    ) -> NodeEdgeCommutationResponse:
        if not response or not response.edges:
            return response
        edges = []
        geometry_line_sources = set()
        geometry_line_to = set()
        for edge in response.edges:
            if not (
                edge.connection_type == "geometry_line" and edge.source_object
            ):
                edges.append(edge)
                continue
            geometry_line_sources.add(
                self.config.get_node_key(edge.source_object)
            )
            geometry_line_to.add(self.config.get_node_key(edge.target))
            geometry_line_to.add(self.config.get_node_key(edge.source))
        if not geometry_line_sources:
            return response

        # nodes
        nodes_query = """
            FOR node IN @@mainCollection
                FILTER node._id IN @nodeIds
                RETURN node
        """
        binds = {
            "nodeIds": list(geometry_line_sources),
            "@mainCollection": self.main_collection.name,
        }
        new_nodes = []
        for node in self.database.aql.execute(nodes_query, bind_vars=binds):
            new_nodes.append(MoNodeResponse.model_validate(node))
        response.nodes.extend(new_nodes)

        # edges
        edges_query = """
            FOR edge IN @@edgeCollection
                FILTER edge._from IN @fromNodes
                FILTER edge._to IN @toNodes
                FILTER edge.source_id IN @fromNodes
                FILTER edge.connection_type IN ["point_a", "point_b"]
                RETURN edge
        """
        binds = {
            "fromNodes": list(geometry_line_sources),
            "toNodes": list(geometry_line_to),
            "@edgeCollection": self.main_edge_collection.name,
        }
        for edge in self.database.aql.execute(edges_query, bind_vars=binds):
            edges.append(MoEdgeResponse.model_validate(edge))
        response.edges = edges

        # tmos
        exclude_tmo_ids = {i.tmo_id for i in response.tmo}
        tmo_ids = {i.tmo for i in new_nodes if i.tmo not in exclude_tmo_ids}
        if tmo_ids:
            tmo_data = [
                TmoResponse.model_validate(
                    tmo.model_dump(mode="json", by_alias=True)
                )
                for tmo in self._get_tmos_data(tmo_ids=list(tmo_ids))
            ]
            response.tmo.extend(tmo_data)
        return response

    def execute(self):
        node_key = f"{self.main_collection.name}/{self.node_key}"
        as_param_tmos = self.group_as_params()
        if not as_param_tmos:
            response = self.get_one_level(node_key=node_key)
        else:
            response = self.get_level_and_table(
                node_key=node_key, as_param_tmos=as_param_tmos
            )
        if self.expand_edges:
            self.replace_with_expanded_edges(response=response)
        response = self.check_response_length(response=response)
        return response

    def get_one_level(self, node_key: str) -> NodeEdgeCommutationResponse:
        children = self._get_child_nodes(node_key=node_key)
        if not children:
            raise NotFound("Children not found")

        tmos = set()
        children_keys = []
        for node in children:
            children_keys.append(node["_key"])
            tmos.add(node["tmo"])
        links = self._get_children_links(
            children_nodes=children_keys,
            neighbour_nodes=self.neighboring_node_keys,
        )
        tmo_data = [
            tmo.model_dump(mode="json", by_alias=True)
            for tmo in self._get_tmos_data(tmo_ids=list(tmos))
        ]
        return NodeEdgeCommutationResponse(
            nodes=children, edges=links, tmo=tmo_data
        )

    def get_level_and_table(
        self, node_key: str, as_param_tmos: list[dict]
    ) -> NodeEdgeCommutationResponse:
        def set_linked_commutations(_nodes: list[dict], _tmo: dict):
            if not _nodes:
                return
            busy_parameter_groups = _tmo.get("busy_parameter_groups", [])
            if not busy_parameter_groups:
                busy_parameter_groups = [None]
            for group in busy_parameter_groups:
                tprms_filter = "FILTER doc.tprm IN @tprmIds" if group else ""
                query = f"""
                    FOR nodeId IN @nodeIds
                        LET _to = (FOR doc IN @@mainEdge
                            FILTER doc._from == nodeId
                            FILTER doc.virtual == False
                            FILTER doc.connection_type
                                IN ["{ConnectionType.MO_LINK.value}", "{ConnectionType.TWO_WAY_MO_LINK.value}"]
                            {tprms_filter}
                            RETURN doc._to)
                        LET connected_with = (FOR doc IN @@mainCollection
                            FILTER doc._id IN _to
                            RETURN DISTINCT doc.{self.commutation_label})
                        RETURN {{"nodeId": nodeId, "connectedWith": connected_with}}
                """
                node_ids = []
                nodes_by_ids = {}
                for i in _nodes:
                    node_ids.append(i["_id"])
                    nodes_by_ids[i["_id"]] = i
                binds = {
                    "@mainEdge": self.main_edge_collection.name,
                    "@mainCollection": self.main_collection.name,
                    "nodeIds": node_ids,
                }
                if group:
                    binds["tprmIds"] = group
                if isinstance(group, list) and len(group) == 0:
                    response_dict = {}
                else:
                    response_dict = {}
                    for item in self.database.aql.execute(
                        query=query, bind_vars=binds
                    ):
                        response_dict[item["nodeId"]] = item["connectedWith"]
                for node_id, _node in nodes_by_ids.items():
                    if not _node.get("connected_with", None):
                        _node["connected_with"] = []
                    busy_group_data = response_dict.get(node_id, [])
                    _node["connected_with"].append(busy_group_data)
            return _nodes

        children = self._get_child_nodes(node_key=node_key)
        if not children:
            raise NotFound("Children not found")

        as_param_tmo_ids = set([i["id"] for i in as_param_tmos])

        parent_node = self.main_collection.get(node_key)
        parent_node_name = parent_node["name"]
        parent_node_label = parent_node.get("label", None)

        unique = []
        not_unique = defaultdict(list)  # tmo_id: list[dict]
        tmos = set()
        for node in children:
            tmos.add(node["tmo"])
            if node["tmo"] not in as_param_tmo_ids:
                unique.append(node)
            else:
                not_unique[node["tmo"]].append(node)
        tmo_data = [
            tmo.model_dump(by_alias=True, mode="json")
            for tmo in self._get_tmos_data(tmo_ids=list(tmos))
        ]

        commutations = []
        for tmo in as_param_tmos:
            tmo_id = tmo["id"]
            if tmo_id not in not_unique:
                continue
            nodes = set_linked_commutations(_nodes=not_unique[tmo_id], _tmo=tmo)

            commutation = CommutationResponse(
                tmo_id=tmo_id,
                tmo_name=tmo["name"],
                parent_name=parent_node_name,
                parent_label=parent_node_label,
                nodes=nodes,
            )

            commutations.append(commutation)

        children_keys = [i["_key"] for i in children]
        links = self._get_children_links(
            children_nodes=children_keys,
            neighbour_nodes=self.neighboring_node_keys,
        )
        return NodeEdgeCommutationResponse(
            nodes=unique, edges=links, commutation=commutations, tmo=tmo_data
        )


class CollapseNodesTask(TaskAbstract, TaskChecks):
    def __init__(self, graph_db: GraphService, key: str, node_key: str):
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

    def _get_parent_node(self, node_id: str):
        query = """
            FOR v, e IN 1..1 OUTBOUND @nodeId GRAPH @mainGraph
                FILTER e.connection_type == "p_id"
                RETURN v
        """
        binds = {
            "nodeId": node_id,
            "mainGraph": self.config.graph_data_graph_name,
        }
        response = list(self.database.aql.execute(query=query, bind_vars=binds))
        if len(response) == 0:
            return None
        return response[0]

    def _get_child_nodes(self, node_id: str):
        query = """
                FOR v, e IN 1..1 INBOUND @nodeId GRAPH @mainGraph
                    FILTER e.connection_type == "p_id"
                    RETURN v
                """
        binds = {
            "nodeId": node_id,
            "mainGraph": self.config.graph_data_graph_name,
        }
        response = list(self.database.aql.execute(query=query, bind_vars=binds))
        return response

    def execute(self):
        node_id = f"{self.config.graph_data_collection_name}/{self.node_key}"
        collapse_to = self._get_parent_node(node_id=node_id)
        if not collapse_to:
            raise NotFound("Parent node not found")
        tmos = [collapse_to["tmo"]]
        tmo_data = [
            tmo.model_dump(mode="json", by_alias=True)
            for tmo in self._get_tmos_data(tmo_ids=tmos)
        ]

        collapse_from = self._get_child_nodes(node_id=collapse_to["_id"])
        return CollapseNodeResponse(
            collapse_from=collapse_from, collapse_to=collapse_to, tmo=tmo_data
        )


class ExpandEdgesTask(TaskAbstract, TaskChecks):
    def __init__(
        self, graph_db: GraphService, key: str, node_key_a: str, node_key_b: str
    ):
        TaskAbstract.__init__(self, graph_db=graph_db, key=key)

        self.node_key_a = node_key_a
        self.node_key_b = node_key_b

    def check(self):
        self.check_status(
            document=self.document, possible_status=[Status.COMPLETE]
        )
        self.check_collection(
            document=self.document, tmo_collection=self.tmo_collection
        )
        self.check_nodes(
            keys=[self.node_key_a, self.node_key_b],
            database=self.database,
            main_collection_name=self.main_collection.name,
        )

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
        query = """
            FOR doc in @@mainEdgeCollection
                FILTER doc._from IN @nodeIds
                FILTER doc._to IN @nodeIds
                FILTER doc._from != doc._to
                RETURN doc
        """
        node_ids = [
            f"{self.main_collection.name}/{self.node_key_a}",
            f"{self.main_collection.name}/{self.node_key_b}",
        ]
        binds = {
            "@mainEdgeCollection": self.config.graph_data_edge_name,
            "nodeIds": node_ids,
        }
        response = self.database.aql.execute(query=query, bind_vars=binds)
        edges = [DbMoEdge.model_validate(i) for i in response]
        nodes, edges = convert_geometry_line(
            edges=edges,
            main_edge_collection=self.main_edge_collection.name,
            main_collection=self.main_collection.name,
            database=self.database,
        )
        tmos = [
            tmo.model_dump(mode="json", by_alias=True)
            for tmo in self._get_tmos_data(tmo_ids=[i.tmo for i in nodes])
        ]
        tprms = self.get_tprms(edges=edges)
        nodes = [
            MoNodeResponse.model_validate(i.model_dump(by_alias=True))
            for i in nodes
        ]
        edges = [
            MoEdgeResponse.model_validate(i.model_dump(by_alias=True))
            for i in edges
        ]
        return NodeEdgeTmoTprmResponse(
            nodes=nodes, edges=edges, tmo=tmos, tprm=tprms
        )


class GetNeighborsTask(TaskAbstract, TaskChecks):
    def __init__(
        self,
        graph_db: GraphService,
        key: str,
        node_key: str,
        n: int,
        with_all_edges: bool,
    ) -> None:
        TaskAbstract.__init__(self, graph_db=graph_db, key=key)

        self.n = n
        self.node_key = node_key
        self.with_all_edges = with_all_edges

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

    def get_neighbors(
        self, node: DbMoNode
    ) -> tuple[list[DbMoNode], [list[DbMoEdge]]]:
        filter_by_real_only = ""
        if not node.grouped_by_tprm:
            filter_by_real_only = "FILTER IS_NULL(v.grouped_by_tprm)"
        query = f"""
            FOR v, e, p IN 1..@n ANY @nodeId GRAPH @mainGraph
                OPTIONS {{uniqueVertices: "global", bfs: true}}
                FILTER p.edges[*].connection_type ALL != "p_id"
                FILTER v.tmo != @traceId
                {filter_by_real_only}
                RETURN {{"node": v, "edge": e}}
                """
        binds = {
            "nodeId": node.id,
            "mainGraph": self.config.graph_data_graph_name,
            "traceId": self.trace_tmo_id,
            "n": self.n,
        }
        nodes: list[DbMoNode] = []
        edges: list[DbMoEdge] = []
        for item in self.database.aql.execute(query=query, bind_vars=binds):
            result_node = DbMoNode.model_validate(item["node"])
            nodes.append(result_node)
            result_edge = DbMoEdge.model_validate(item["edge"])
            edges.append(result_edge)
        return nodes, edges

    def drop_parents(
        self, nodes: list[DbMoNode], edges: list[DbMoEdge]
    ) -> tuple[list[DbMoNode], list[DbMoEdge]]:
        if len(nodes) == 0:
            return nodes, edges
        current_node_id: str | None = self.config.get_node_key(self.node_key)
        query = """
            FOR edge in @@mainEdgeCollection
                FILTER edge.connection_type == "p_id"
                FILTER edge._from == @currentNodeId
                LIMIT 1
                RETURN edge._to
        """
        binds = {"@mainEdgeCollection": self.main_edge_collection.name}
        parents = set()
        while current_node_id:
            binds["currentNodeId"] = current_node_id
            response = list(
                self.database.aql.execute(query=query, bind_vars=binds)
            )
            if not response:
                break
            parent = response[0]
            parents.add(parent)
            current_node_id = parent

        if not parents:
            return nodes, edges

        nodes = [node for node in nodes if node.id not in parents]
        edges = [
            edge
            for edge in edges
            if edge.from_ not in parents and edge.to_ not in parents
        ]
        return nodes, edges

    def drop_children_nodes(
        self, nodes: list[DbMoNode], edges: list[DbMoEdge]
    ) -> tuple[list[DbMoNode], [list[DbMoEdge]]]:
        if len(nodes) == 0:
            return nodes, edges
        query = """
            FOR doc IN @@edgeCollection
                FILTER doc._from IN @ids
                FILTER doc._to IN @ids
                FILTER doc.connection_type == "p_id"
                RETURN doc._from
        """
        ids = [i.id for i in nodes]
        ids.append(self.config.get_node_key(self.node_key))
        binds = {"@edgeCollection": self.main_edge_collection.name, "ids": ids}
        response = set(self.database.aql.execute(query=query, bind_vars=binds))
        if response:
            cleared_nodes = []
            for node in nodes:
                if node.id in response:
                    continue
                cleared_nodes.append(node)
            nodes = cleared_nodes

            cleared_edges = []
            for edge in edges:
                if edge.from_ in response:
                    continue
                if edge.to_ in response:
                    continue
                cleared_edges.append(edge)
            edges = cleared_edges
        return nodes, edges

    def find_all_edges(
        self, nodes: list[DbMoNode], edges: list[DbMoEdge]
    ) -> tuple[list[DbMoNode], [list[DbMoEdge]]]:
        query = """
            FOR edge in @@edgeCollection
                FILTER edge._from IN @ids
                FILTER edge._to IN @ids
                RETURN edge
        """
        ids = [i.id for i in nodes]
        current_node_id: str | None = self.config.get_node_key(self.node_key)
        ids.append(current_node_id)
        binds = {"@edgeCollection": self.main_edge_collection.name, "ids": ids}
        response = self.database.aql.execute(query=query, bind_vars=binds)
        edges = [DbMoEdge.model_validate(i) for i in response]
        return nodes, edges

    @staticmethod
    def drop_geometry_type_line_nodes(
        nodes: list[DbMoNode], edges: list[DbMoEdge]
    ) -> tuple[list[DbMoNode], [list[DbMoEdge]]]:
        source_ids = {edge.source_id for edge in edges if edge.source_id}
        if not source_ids:
            return nodes, edges
        nodes = [node for node in nodes if node.id not in source_ids]
        return nodes, edges

    def drop_geometry_type_line_edges(
        self,
        nodes: list[DbMoNode],
        edges: list[DbMoEdge],
    ) -> tuple[list[DbMoNode], [list[DbMoEdge]]]:
        source_ids = {node.id for node in nodes}
        current_node_id: str | None = self.config.get_node_key(self.node_key)
        source_ids.add(current_node_id)
        if not source_ids:
            return nodes, edges
        edges = [
            edge
            for edge in edges
            if not (
                edge.connection_type == "geometry_line"
                and edge.source_id in source_ids
            )
        ]
        return nodes, edges

    def execute(self):
        node = DbMoNode.model_validate(
            self.main_collection.get({"_key": self.node_key})
        )
        nodes, edges = self.get_neighbors(node=node)
        nodes, edges = self.drop_parents(nodes=nodes, edges=edges)
        nodes, edges = self.drop_children_nodes(nodes=nodes, edges=edges)
        if self.with_all_edges:
            nodes, edges = self.find_all_edges(nodes=nodes, edges=edges)
            nodes, edges = self.drop_geometry_type_line_edges(
                nodes=nodes, edges=edges
            )
        else:
            nodes, edges = self.drop_geometry_type_line_nodes(
                nodes=nodes, edges=edges
            )
        tmos = [
            tmo.model_dump(mode="json", by_alias=True)
            for tmo in self._get_tmos_data(tmo_ids=[i.tmo for i in nodes])
        ]
        response = NodeEdgeCommutationResponse(
            nodes=[
                MoNodeResponse.model_validate(i.model_dump(by_alias=True))
                for i in nodes
            ],
            edges=[
                MoEdgeResponse.model_validate(i.model_dump(by_alias=True))
                for i in edges
            ],
            tmo=tmos,
        )
        return response
