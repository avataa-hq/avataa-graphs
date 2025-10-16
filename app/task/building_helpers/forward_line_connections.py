from collections import defaultdict
from typing import Iterator

from task.building_helpers.build_from_tmo import QUERY_ITEMS_LIMIT
from task.building_helpers.spread_connections import spread_connections
from task.models.dto import DbMoEdge, DbTmoNode, MoEdge
from task.models.enums import ConnectionType
from task.task_abstract import TaskAbstract


def get_line_tmos(task: TaskAbstract) -> list[DbTmoNode]:
    line_tmo_query = """
        FOR doc IN @@tmoCollection
            FILTER doc.geometry_type == 'line'
            RETURN doc
    """
    binds = {"@tmoCollection": task.tmo_collection.name}
    return [
        DbTmoNode.model_validate(i)
        for i in task.database.aql.execute(
            query=line_tmo_query, bind_vars=binds
        )
    ]


def create_link(parent_id: str, point_a_id: str, point_b_id: str) -> MoEdge:
    result = MoEdge(
        _from=point_a_id,
        _to=point_b_id,
        connection_type=ConnectionType.GEOMETRY_LINE,
        is_trace=False,
        virtual=True,
        source_id=parent_id,
    )
    return result


def create_trace_link(
    _to: str,
    _from: str,
    link_type: str,
    task: TaskAbstract,
    source_id: str | None = None,
) -> MoEdge:
    return MoEdge.model_validate(
        {
            "_from": _from,
            "_to": _to,
            "connection_type": link_type,
            "is_trace": True,
            "virtual": True,
            "source_id": source_id,
        }
    )


def get_line_connections(
    task: TaskAbstract, tmo_ids: list[int]
) -> Iterator[list[MoEdge]]:
    limit = QUERY_ITEMS_LIMIT
    query = """
        FOR doc IN @@mainCollection
            FILTER doc.tmo IN @tmoIds
            FILTER NOT_NULL(doc.data.point_a_id)
            FILTER NOT_NULL(doc.data.point_b_id)
            LET point_a = FIRST(
                FOR edge IN @@mainEdgeCollection
                    FILTER edge._from == doc._id
                    FILTER edge.connection_type == "point_a"
                    FILTER edge.virtual == false
                    LIMIT 1
                    RETURN edge._to
            )
            FILTER NOT_NULL(point_a)
            LET point_b = FIRST(
                FOR edge IN @@mainEdgeCollection
                    FILTER edge._from == doc._id
                    FILTER edge.connection_type == "point_b"
                    FILTER edge.virtual == false
                    LIMIT 1
                    RETURN edge._to
            )
            FILTER NOT_NULL(point_b)
            LIMIT @offset, @limit
            RETURN {"parent_id": doc._id, "point_a_id": point_a, "point_b_id": point_b}
        """

    binds = {
        "@mainCollection": task.main_collection.name,
        "limit": limit,
        "tmoIds": tmo_ids,
        "@mainEdgeCollection": task.main_edge_collection.name,
    }
    offset = 0
    last_response_len = limit
    while limit <= last_response_len:
        binds["offset"] = offset
        response = task.database.aql.execute(query=query, bind_vars=binds)
        chunk_mo_links = [
            create_link(
                parent_id=i["parent_id"],
                point_a_id=i["point_a_id"],
                point_b_id=i["point_b_id"],
            )
            for i in response
        ]
        last_response_len = len(chunk_mo_links)
        offset += last_response_len
        yield chunk_mo_links


def create_trace_links_in_line_connections(
    task: TaskAbstract, line_edges: list[DbMoEdge]
) -> list[DbMoEdge]:
    if not line_edges:
        return []
    query = """
        FOR edge IN @@mainEdgeCollection
            FILTER edge._from IN @lineEdgeIds
            FILTER edge.is_trace == true
            RETURN edge
    """
    line_edge_ids = defaultdict(list)
    for edge in line_edges:
        if (
            not edge.source_id
            or edge.connection_type != ConnectionType.GEOMETRY_LINE
        ):
            continue
        line_edge_ids[edge.source_id].append(edge)
    binds = {
        "@mainEdgeCollection": task.main_edge_collection.name,
        "lineEdgeIds": list(line_edge_ids.keys()),
    }
    response = task.database.aql.execute(query=query, bind_vars=binds)
    db_link_edges = [DbMoEdge.model_validate(i) for i in response]
    if not db_link_edges:
        return []
    new_links = []
    for db_link_edge in db_link_edges:
        db_links = line_edge_ids[db_link_edge.from_]
        for db_link in db_links:  # type: DbMoEdge
            _from_links = (db_link.from_, db_link.to_)
            for _from_link in _from_links:
                new_link = create_trace_link(
                    _to=db_link_edge.to_,
                    _from=_from_link,
                    link_type=ConnectionType.MO_LINK.value,
                    task=task,
                    source_id=db_link_edge.from_,
                )
                new_links.append(new_link)
    result = []
    for db_edge in task.main_edge_collection.insert_many(
        documents=[i.model_dump(mode="json", by_alias=True) for i in new_links],
        keep_none=True,
        return_new=True,
    ):
        db_edge = DbMoEdge.model_validate(db_edge["new"])
        result.append(db_edge)
    return result


def forward_line_connections(task: TaskAbstract):
    line_tmo = get_line_tmos(task=task)
    tmo_ids = [i.tmo_id for i in line_tmo]
    for chunk_edges in get_line_connections(task=task, tmo_ids=tmo_ids):
        if not chunk_edges:
            continue
        chunk_edges = [
            i.model_dump(mode="json", by_alias=True) for i in chunk_edges
        ]
        db_edges = []
        for db_edge in task.main_edge_collection.insert_many(
            documents=chunk_edges, keep_none=True, return_new=True
        ):
            db_edge = DbMoEdge.model_validate(db_edge["new"])
            db_edges.append(db_edge)
        trace_db_edges = create_trace_links_in_line_connections(
            task=task, line_edges=db_edges
        )
        db_edges.extend(trace_db_edges)
        spread_connections(edges=db_edges, task=task)
