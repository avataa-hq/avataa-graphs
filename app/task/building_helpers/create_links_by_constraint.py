from collections import defaultdict
from typing import Iterator, Literal

from arango import DocumentInsertError

from task.models.building import ConstraintFilter
from task.models.dto import DbMoEdge, DbMoNode, DbTmoNode, MoEdge
from task.models.enums import ConnectionType, LinkType
from task.models.errors import GraphBuildingError
from task.task_abstract import TaskAbstract


def inverse_connections(
    query: str, binds: dict, task: TaskAbstract, limit: int = 1000
) -> dict[int, list[dict]]:
    reversed_connections = defaultdict(list)
    binds["limit"] = limit
    offset = 0
    last_response_size = limit
    while last_response_size >= limit:
        binds["offset"] = offset
        response = list(task.database.aql.execute(query=query, bind_vars=binds))
        last_response_size = len(response)
        offset += last_response_size

        for connections in response:
            if isinstance(connections["to_mo_id"], list):
                for to_connection in connections["to_mo_id"]:
                    reversed_connections[to_connection].append(connections)
            else:
                reversed_connections[connections["to_mo_id"]].append(
                    connections
                )
    return reversed_connections


def save_edges(edges: list[MoEdge], task: TaskAbstract) -> list[DbMoEdge]:
    db_mo_edges = []
    if not edges:
        return db_mo_edges
    edges = [i.model_dump(mode="json", by_alias=True) for i in edges]
    for item in task.main_edge_collection.insert_many(
        documents=edges, return_new=True
    ):
        if isinstance(item, DocumentInsertError):
            raise GraphBuildingError(f"Edge insertion error. {str(item)}")
        db_mo_edge = DbMoEdge.model_validate(item["new"])
        db_mo_edges.append(db_mo_edge)
    return db_mo_edges


def find_nodes_by_mo_ids(
    task: TaskAbstract,
    constraint_filter: ConstraintFilter,
    mo_ids: list[int],
    chunk_size: int = 100,
) -> Iterator[list[DbMoNode]]:
    query = """
        FOR doc IN @@mainCollection
            FILTER doc.data.id IN @moIds
            FILTER doc.tmo IN @tmoIds
            LIMIT @offset, @limit
            RETURN doc
    """
    last_size = chunk_size
    offset = 0
    binds = {
        "offset": offset,
        "limit": chunk_size,
        "moIds": mo_ids,
        "tmoIds": constraint_filter.to_tmo_id,
        "@mainCollection": task.config.graph_data_collection_name,
    }
    while last_size >= chunk_size:
        binds["offset"] = offset
        response = task.database.aql.execute(query=query, bind_vars=binds)
        chunk = [DbMoNode.model_validate(i) for i in response]
        last_size = len(chunk)
        offset += last_size
        yield chunk


def find_links(
    task: TaskAbstract,
    tmo: DbTmoNode,
    constraint_filter: ConstraintFilter,
    connection_type: ConnectionType,
) -> Iterator[list[MoEdge]]:
    query = """
        FOR doc IN @@mainCollection
            FILTER doc.tmo == @tmoId
            FILTER NOT_NULL(doc.data.params)
            FOR param in doc.data.params
                FILTER param.tprm_id == @tprmId
                LIMIT @offset, @limit
                RETURN {"_from": doc._id, "to_mo_id": param.value, "prm_id": param.id, "tprm_id": param.tprm_id}
    """
    binds = {
        "@mainCollection": task.config.graph_data_collection_name,
        "tprmId": constraint_filter.tprm_id,
        "tmoId": tmo.tmo_id,
    }
    inverted_connections = inverse_connections(
        query=query, binds=binds, task=task
    )
    if not inverted_connections:
        return

    for chunk_to in find_nodes_by_mo_ids(
        mo_ids=list(inverted_connections.keys()),
        task=task,
        constraint_filter=constraint_filter,
    ):
        # create edges
        chunk_results = []
        for node_to in chunk_to:
            for from_ in inverted_connections[node_to.data.id]:
                edge = MoEdge(
                    _from=from_["_from"],
                    _to=node_to.id,
                    connection_type=connection_type,
                    prm=[from_["prm_id"]],
                    tprm=from_["tprm_id"],
                    is_trace=node_to.is_trace,
                    virtual=False,
                    source_id=from_["_from"],
                )
                chunk_results.append(edge)
        yield chunk_results


def point_links(
    point: Literal["a", "b"],
    task: TaskAbstract,
    constraint_filter: ConstraintFilter,
    tmo: DbTmoNode,
) -> Iterator[list[MoEdge]]:
    query = f"""
        FOR doc IN @@mainCollection
            FILTER doc.tmo == @tmoId
            FILTER NOT_NULL(doc.data.point_{point}_id)
            LIMIT @offset, @limit
            RETURN {{"_from": doc._id, "to_mo_id": doc.data.point_{point}_id}}
    """
    binds = {
        "@mainCollection": task.config.graph_data_collection_name,
        "tmoId": tmo.tmo_id,
    }
    inverted_connections = inverse_connections(
        query=query, binds=binds, task=task
    )
    if not inverted_connections:
        return

    for chunk_to in find_nodes_by_mo_ids(
        mo_ids=list(inverted_connections.keys()),
        task=task,
        constraint_filter=constraint_filter,
    ):
        # create edges
        chunk_results = []
        for node_to in chunk_to:
            for from_ in inverted_connections[node_to.data.id]:
                edge = MoEdge(
                    _from=from_["_from"],
                    _to=node_to.id,
                    connection_type=ConnectionType.POINT_A
                    if point == "a"
                    else ConnectionType.POINT_B,
                    prm=None,
                    tprm=None,
                    is_trace=node_to.is_trace,
                    virtual=False,
                    source_id=from_["_from"],
                )
                chunk_results.append(edge)
        yield chunk_results


def create_links_by_constraint(
    task: TaskAbstract, tmo: DbTmoNode, constraint_filter: ConstraintFilter
):
    match constraint_filter.link_type:
        case LinkType.MO_LINK:
            for edges_chunk in find_links(
                task=task,
                tmo=tmo,
                constraint_filter=constraint_filter,
                connection_type=ConnectionType.MO_LINK,
            ):
                save_edges(edges=edges_chunk, task=task)
        case LinkType.TWO_WAY_MO_LINK:
            for edges_chunk in find_links(
                task=task,
                tmo=tmo,
                constraint_filter=constraint_filter,
                connection_type=ConnectionType.TWO_WAY_MO_LINK,
            ):
                save_edges(edges=edges_chunk, task=task)
        case LinkType.POINT_CONSTRAINT:
            for edges_chunk in point_links(
                point="a",
                task=task,
                constraint_filter=constraint_filter,
                tmo=tmo,
            ):
                save_edges(edges=edges_chunk, task=task)
            for edges_chunk in point_links(
                point="b",
                task=task,
                constraint_filter=constraint_filter,
                tmo=tmo,
            ):
                save_edges(edges=edges_chunk, task=task)
        case _:
            raise GraphBuildingError(
                "Edge creation error. Link type not supported"
            )
