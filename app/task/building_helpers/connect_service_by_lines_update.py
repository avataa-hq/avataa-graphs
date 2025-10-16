from typing import Iterator

from task.building_helpers.build_from_tmo import QUERY_ITEMS_LIMIT
from task.building_helpers.connect_service_by_lines import create_edge_to_trace
from task.building_helpers.create_links_by_constraint import save_edges
from task.models.dto import DbMoEdge, MoEdge
from task.models.enums import ConnectionType
from task.task_abstract import TaskAbstract


def find_edges_with_connection_type_geometry_line_trace(
    task: TaskAbstract, edges: list[DbMoEdge]
) -> Iterator[dict]:
    query = f"""
        LET serviceIds = (
            FOR node IN @@mainCollection
                FILTER node.tmo == @traceTmo
                RETURN node._id
        )

        FOR edge in @@mainEdgeCollection
            FILTER edge.connection_type == 'geometry_line'
            LET services_a = (
                FOR s_edge IN @@mainEdgeCollection
                    FILTER s_edge.connection_type
                        IN ["{ConnectionType.MO_LINK.value}", "{ConnectionType.TWO_WAY_MO_LINK.value}"]
                    FILTER s_edge._from == edge._from
                    FILTER s_edge._to IN serviceIds
                    RETURN s_edge
                )
            LET services_b = (
                FOR s_edge IN @@mainEdgeCollection
                    FILTER s_edge.connection_type
                        IN ["{ConnectionType.MO_LINK.value}", "{ConnectionType.TWO_WAY_MO_LINK.value}"]
                    FILTER s_edge._from == edge._to
                    FILTER s_edge._to IN serviceIds
                    RETURN s_edge
                )
            LET check_source = (
                FOR source_edge IN UNION(services_a, services_b)
                    FILTER source_edge.source_id IN @sourceIds
                    LIMIT 1
                    RETURN source_edge
            )
            FILTER LENGTH(check_source) > 0
            LET a_to_ids = (
                FOR doc IN services_a
                    RETURN DISTINCT doc._to
            )
            LET b_to_ids = (
                FOR doc IN services_b
                    RETURN DISTINCT doc._to
                )
            LET services_to_ids = INTERSECTION(a_to_ids, b_to_ids)
            FILTER LENGTH(services_to_ids) > 0
            LET filtered_a = (
                FOR doc IN services_a
                    FILTER doc._to IN services_to_ids
                    RETURN doc
            )
            LET filtered_b = (
                FOR doc IN services_b
                    FILTER doc._to IN services_to_ids
                    RETURN doc
            )
            LET services = UNION(filtered_a, filtered_b)
            LIMIT @offset, @limit
            RETURN {{"source_id": edge.source_id, "services": services}}
    """
    limit = QUERY_ITEMS_LIMIT
    source_ids = list(set([i.source_id for i in edges if i.source_id]))
    binds = {
        "@mainEdgeCollection": task.main_edge_collection.name,
        "@mainCollection": task.main_collection.name,
        "traceTmo": task.trace_tmo_id,
        "limit": limit,
        "sourceIds": source_ids,
    }
    offset = 0
    last_response_len = limit
    while last_response_len >= limit:
        binds["offset"] = offset
        response = list(task.database.aql.execute(query=query, bind_vars=binds))
        last_response_len = len(response)
        offset += last_response_len
        for item in response:
            item = {
                "source_id": item["source_id"],
                "services": [
                    DbMoEdge.model_validate(i) for i in item["services"]
                ],
            }
            yield item


def check_same_edge_exists(task: TaskAbstract, edge: MoEdge) -> bool:
    query = """
        FOR edge in @@mainEdgeCollection
            FILTER edge._from == @from
            FILTER edge._to == @to
            FILTER edge.connection_type == @connectionType
            FILTER edge.is_trace == @isTrace
            FILTER edge.virtual == @virtual
            FILTER edge.source_id == @sourceId
            LIMIT 1
            RETURN edge
    """
    binds = {
        "@mainEdgeCollection": task.main_edge_collection.name,
        "from": edge.from_,
        "to": edge.to_,
        "connectionType": edge.connection_type,
        "isTrace": edge.is_trace,
        "virtual": edge.virtual,
        "sourceId": edge.source_id,
    }
    response = list(task.database.aql.execute(query=query, bind_vars=binds))
    return len(response) > 0


def connect_service_by_lines_update(task: TaskAbstract, edges: list[DbMoEdge]):
    if not edges:
        return
    if not task.trace_tmo_id:
        return
    edge_iterator = find_edges_with_connection_type_geometry_line_trace(
        task, edges=edges
    )
    buffer = []
    buffer_size = QUERY_ITEMS_LIMIT
    for edge in edge_iterator:
        for service in edge["services"]:
            trace_edge = create_edge_to_trace(
                source_id=edge["source_id"], service_edge=service
            )
            if check_same_edge_exists(task=task, edge=trace_edge):
                continue
            buffer.append(trace_edge)
        if len(buffer) >= buffer_size:
            save_edges(edges=buffer, task=task)
            buffer = []
    if buffer:
        save_edges(edges=buffer, task=task)
