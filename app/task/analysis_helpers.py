from collections import defaultdict

from fastapi import HTTPException
from starlette.status import HTTP_510_NOT_EXTENDED

from config import GraphDBConfig
from task.models.outgoing_data import (
    MoEdgeResponse,
    MoNodeResponse,
    NodeEdgeCommutationResponse,
    NodeEdgeErrorResponse,
)
from task.task_abstract import TaskAbstract


def check_response_length(
    response: NodeEdgeCommutationResponse, size: int
) -> NodeEdgeCommutationResponse | NodeEdgeErrorResponse:
    if 0 < size < response.size:
        error_response = NodeEdgeErrorResponse(
            description="Response size exceeded. Specify your request parameters",
            params={"size": response.size, "max_size": size},
        )
        raise HTTPException(
            status_code=HTTP_510_NOT_EXTENDED,
            detail=error_response.model_dump(mode="json"),
        )
    return response


def append_point_edges(
    response: NodeEdgeCommutationResponse,
    task: TaskAbstract,
    config: GraphDBConfig,
) -> NodeEdgeCommutationResponse:
    line_tmos = {i.tmo_id: i for i in response.tmo if i.geometry_type == "line"}
    if not line_tmos:
        return response
    nodes_by_tmo = defaultdict(list)
    node_keys = set()
    for node in response.nodes:
        if node.tmo not in line_tmos:
            continue
        nodes_by_tmo[node.tmo].append(node)
        node_keys.add(node.key)
    edges_by_source = defaultdict(set)
    for edge in response.edges:
        if (
            edge.source in node_keys
            and edge.connection_type in ("point_a", "point_b")
            and not edge.virtual
        ):
            edges_by_source[edge.source].add(edge.connection_type)
    need_a_links = []
    need_b_links = []
    for tmo_id, nodes in nodes_by_tmo.items():
        for node in nodes:
            points_set = edges_by_source.get(node.key, set())
            if "point_a" not in points_set:
                need_a_links.append(config.get_node_key(node.key))
            if "point_b" not in points_set:
                need_b_links.append(config.get_node_key(node.key))
    edge_query = """
        FOR edge IN @@mainEdgeCollection
            FILTER edge._from IN @nodeIds
            FILTER edge.connection_type == @connectionType
            FILTER edge.virtual == false
            LET node = FIRST(FOR node in @@mainCollection
                FILTER node._id == edge._to
                LIMIT 1
                RETURN node)
            RETURN {"edge": edge, "node": node}
    """
    new_links = []
    new_nodes = []
    if need_a_links:
        edge_binds = {
            "@mainEdgeCollection": task.main_edge_collection.name,
            "@mainCollection": task.main_collection.name,
            "connectionType": "point_a",
            "nodeIds": need_a_links,
        }
        db_response = task.database.aql.execute(
            query=edge_query, bind_vars=edge_binds
        )
        links = []
        nodes = []
        for i in db_response:
            link = MoEdgeResponse.model_validate(i["edge"])
            links.append(link)
            node = MoNodeResponse.model_validate(i["node"])
            nodes.append(node)
        new_links.extend(links)
        new_nodes.extend(nodes)

    if need_b_links:
        edge_binds = {
            "@mainEdgeCollection": task.main_edge_collection.name,
            "@mainCollection": task.main_collection.name,
            "connectionType": "point_b",
            "nodeIds": need_b_links,
        }
        db_response = task.database.aql.execute(
            query=edge_query, bind_vars=edge_binds
        )
        links = []
        nodes = []
        for i in db_response:
            link = MoEdgeResponse.model_validate(i["edge"])
            links.append(link)
            node = MoNodeResponse.model_validate(i["node"])
            nodes.append(node)
        new_links.extend(links)
        new_nodes.extend(nodes)
    if new_links:
        response.edges.extend(new_links)
    if new_nodes:
        response.nodes.extend(new_nodes)
    return response
