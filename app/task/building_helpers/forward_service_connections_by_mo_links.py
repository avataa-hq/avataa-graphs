from collections import defaultdict
from typing import Iterator

from arango import DocumentInsertError

from task.building_helpers.build_from_tmo import QUERY_ITEMS_LIMIT
from task.building_helpers.spread_connections import spread_connections
from task.models.dto import DbMoEdge, MoEdge
from task.models.errors import GraphBuildingError
from task.task_abstract import TaskAbstract


def get_nodes_to_trace_edges(task: TaskAbstract) -> Iterator[DbMoEdge]:
    limit = QUERY_ITEMS_LIMIT
    query = """
        FOR edge IN @@mainEdgeCollection
            FILTER edge.is_trace == true
            FILTER edge.virtual == false
            LIMIT @offset, @limit
            RETURN edge
    """
    binds = {
        "@mainEdgeCollection": task.main_edge_collection.name,
        "limit": limit,
    }

    offset = 0
    last_response_length = limit
    while limit <= last_response_length:
        binds["offset"] = offset
        response = task.database.aql.execute(query=query, bind_vars=binds)
        result = [DbMoEdge.model_validate(i) for i in response]
        yield from result
        last_response_length = len(result)
        offset += last_response_length


def create_links_chunk(
    task: TaskAbstract, mo_link_edge_ids_dict: dict[str, list[DbMoEdge]]
) -> list[DbMoEdge]:
    new_links = []
    for node_id, mo_link_edges in mo_link_edge_ids_dict.items():
        for mo_link_edge in mo_link_edges:
            _to = mo_link_edge.to_
            source_id = mo_link_edge.from_
            new_link = MoEdge.model_validate(
                dict(
                    _from=node_id,
                    _to=_to,
                    connection_type=mo_link_edge.connection_type,
                    prm=mo_link_edge.prm,
                    tprm=mo_link_edge.tprm,
                    is_trace=mo_link_edge.is_trace,
                    virtual=True,
                    source_id=source_id,
                )
            )
            new_links.append(new_link)
    results = []
    for db_item in task.main_edge_collection.insert_many(
        [i.model_dump(mode="json", by_alias=True) for i in new_links],
        return_new=True,
        keep_none=True,
    ):
        if isinstance(db_item, DocumentInsertError):
            raise GraphBuildingError(f"Edge insertion error. {str(db_item)}")
        result = DbMoEdge.model_validate(db_item["new"])
        results.append(result)
    return results


def forward_service_connections_by_mo_links_chunk(
    task: TaskAbstract, service_edges: list[DbMoEdge]
) -> list[DbMoEdge]:
    if not service_edges:
        return []
    query = """
        FOR edge IN @@mainEdgeCollection
            FILTER edge.connection_type == "mo_link"
            FILTER edge.is_trace == false
            FILTER edge._from IN @nodeIds
            RETURN edge
    """
    service_ids_dict = defaultdict(list)
    for service_edge in service_edges:
        if not service_edge.is_trace:
            continue
        service_ids_dict[service_edge.from_].append(service_edge)
    binds = {
        "@mainEdgeCollection": task.main_edge_collection.name,
        "nodeIds": list(service_ids_dict.keys()),
    }
    response = task.database.aql.execute(query=query, bind_vars=binds)
    mo_link_edges: list[DbMoEdge] = [
        DbMoEdge.model_validate(i) for i in response
    ]
    mo_link_edge_ids_dict = defaultdict(list)
    for mo_link_edge in mo_link_edges:
        mo_link_edge_ids_dict[mo_link_edge.to_].extend(
            service_ids_dict[mo_link_edge.from_]
        )
    links = create_links_chunk(
        mo_link_edge_ids_dict=mo_link_edge_ids_dict, task=task
    )
    spread_connections(task=task, edges=links)
    return links


def forward_service_connections_by_mo_links(
    task: TaskAbstract, service_edges: list[DbMoEdge] | None = None
):
    if not service_edges:
        tmo_node = task.trace_tmo_data
        if not tmo_node:
            return
        service_edges = get_nodes_to_trace_edges(task=task)

    buffer_size = QUERY_ITEMS_LIMIT
    buffer = []
    for service_edge in service_edges:
        buffer.append(service_edge)
        if len(buffer) < buffer_size:
            continue
        forward_service_connections_by_mo_links_chunk(
            task=task, service_edges=buffer
        )
        buffer = []
    if buffer:
        forward_service_connections_by_mo_links_chunk(
            task=task, service_edges=buffer
        )
