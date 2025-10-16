from collections import defaultdict
from typing import Iterator

from arango import DocumentInsertError

from task.building_helpers.build_from_tmo import QUERY_ITEMS_LIMIT
from task.models.dto import DbMoEdge, DbMoNode, MoEdge
from task.task_abstract import TaskAbstract


def _get_children_edges(
    task: TaskAbstract, node_ids: list[str]
) -> list[DbMoEdge]:
    query = """
        FOR edge IN @@moEdgeCollection
        FILTER edge._to IN @nodeIds
        FILTER edge.connection_type == "p_id"
        RETURN edge
    """
    binds = {
        "@moEdgeCollection": task.main_edge_collection.name,
        "nodeIds": node_ids,
    }
    results = [
        DbMoEdge.model_validate(i)
        for i in task.database.aql.execute(query=query, bind_vars=binds)
    ]
    return results


def _get_parent_edges(
    task: TaskAbstract, node_ids: list[str]
) -> list[DbMoEdge]:
    query = """
        FOR edge IN @@moEdgeCollection
        FILTER edge._from IN @nodeIds
        FILTER edge.connection_type == "p_id"
        RETURN edge
    """
    binds = {
        "@moEdgeCollection": task.main_edge_collection.name,
        "nodeIds": node_ids,
    }
    results = [
        DbMoEdge.model_validate(i)
        for i in task.database.aql.execute(query=query, bind_vars=binds)
    ]
    return results


def _get_db_mo_nodes(
    task: TaskAbstract,
    tmo_id: int,
    tprm_id: int | None,
    static_offset: int | None = None,
) -> Iterator[list[DbMoNode]]:
    limit = QUERY_ITEMS_LIMIT
    offset = 0 if static_offset is None else static_offset
    last_response_len = limit

    binds = {
        "@moNodeCollection": task.main_collection.name,
        "tmoId": tmo_id,
        "limit": limit,
    }
    tprm_filter = "FILTER IS_NULL(node.grouped_by_tprm)"
    if tprm_id:
        tprm_filter = "FILTER node.grouped_by_tprm == @tprmId"
        binds["tprmId"] = tprm_id
    query = f"""
        FOR node IN @@moNodeCollection
            FILTER node.tmo == @tmoId
            {tprm_filter}
            LIMIT @offset, @limit
            RETURN node
    """
    while last_response_len >= limit:
        binds["offset"] = offset
        response = task.database.aql.execute(query=query, bind_vars=binds)
        result = [DbMoNode.model_validate(i) for i in response]
        if result:
            yield result
        else:
            return
        if static_offset is None:
            offset += len(result)


def reconnect_p_id_links(
    parent_edges: dict[str, DbMoEdge], children_edges: dict[str, list[DbMoEdge]]
) -> list[MoEdge]:
    results: list[MoEdge] = []
    for key, children_edges_group in children_edges.items():
        parent_edge = parent_edges.get(key, None)
        if not parent_edge:
            continue
        for child_edge in children_edges_group:  # type: DbMoEdge
            new_link = MoEdge(
                _from=child_edge.from_,
                _to=parent_edge.to_,
                connection_type=child_edge.connection_type,
                prm=child_edge.prm,
                tprm=child_edge.tprm,
                is_trace=child_edge.is_trace,
                virtual=child_edge.virtual,
                source_id=child_edge.source_id,
            )
            results.append(new_link)
    return results


def delete_edges_with_source_id(task: TaskAbstract, node_ids: list[str]):
    query = """
        FOR node IN @@moEdgeCollection
            FILTER node.source_id IN @nodeIds
            REMOVE node IN @@moEdgeCollection
    """
    binds = {
        "@moEdgeCollection": task.main_edge_collection.name,
        "nodeIds": node_ids,
    }
    task.database.aql.execute(query=query, bind_vars=binds)


def delete_edges_by_node_id(task: TaskAbstract, node_ids: list[str]):
    query = """
            FOR node IN @@moEdgeCollection
                FILTER (node._from IN @nodeIds OR node._to IN @nodeIds)
                REMOVE node IN @@moEdgeCollection
        """
    binds = {
        "@moEdgeCollection": task.main_edge_collection.name,
        "nodeIds": node_ids,
    }
    task.database.aql.execute(query=query, bind_vars=binds)


def _get_group_by_tprms(task: TaskAbstract, tmo_id: int) -> list[int | None]:
    query = """
        FOR node IN @@mainCollection
            FILTER node.tmo = @tmo_id
            RETURN DISTINCT node.grouped_by_tprm
    """
    binds = {"@mainCollection": task.main_collection.name, "tmo_id": tmo_id}

    grouped_by_tprms = list(
        task.database.aql.execute(query=query, bind_vars=binds)
    )
    return grouped_by_tprms


def drop_tmo_from_mo_collection(
    task: TaskAbstract, tmo_id: int, tprm_ids: list[int | None] | None
) -> None:
    if not tprm_ids:
        tprm_ids = _get_group_by_tprms(task=task, tmo_id=tmo_id)
    for tprm_id in tprm_ids:
        node_ids_to_delete = []
        for db_nodes_chunk in _get_db_mo_nodes(
            task=task, tmo_id=tmo_id, tprm_id=tprm_id
        ):
            chunk_node_ids = [i.id for i in db_nodes_chunk]
            parent_edge_by_from = {
                i.from_: i
                for i in _get_parent_edges(node_ids=chunk_node_ids, task=task)
            }
            children_edge_by_to = defaultdict(list)
            for child_edge in _get_children_edges(
                task=task, node_ids=chunk_node_ids
            ):
                children_edge_by_to[child_edge.to_].append(child_edge)
            new_edges = reconnect_p_id_links(
                parent_edges=parent_edge_by_from,
                children_edges=children_edge_by_to,
            )

            # delete
            delete_edges_with_source_id(node_ids=chunk_node_ids, task=task)
            delete_edges_by_node_id(task=task, node_ids=chunk_node_ids)
            node_ids_to_delete.extend(chunk_node_ids)

            if new_edges:
                for edge in task.main_edge_collection.insert_many(
                    [
                        i.model_dump(mode="json", by_alias=True)
                        for i in new_edges
                    ]
                ):
                    if isinstance(edge, DocumentInsertError):
                        raise ValueError("Edge insert error " + str(edge))
        task.main_collection.delete_many(node_ids_to_delete)  # type: ignore # noqa: F401
