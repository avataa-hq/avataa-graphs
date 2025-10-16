from __future__ import annotations

from dataclasses import dataclass, field
from typing import Iterator, TypeAlias

from task.building_helpers.build_from_tmo import QUERY_ITEMS_LIMIT
from task.models.dto import DbMoNode
from task.models.outgoing_data import TmoConfigResponse
from task.task_abstract import TaskAbstract
from task.tmo_tasks import TmoTask

NodeId: TypeAlias = str
Breadcrumb: TypeAlias = str


@dataclass(slots=True)
class TmoFilter:
    tmo_id: int
    tprm_id: int | None = None

    parent: TmoFilter = None
    children: list[TmoFilter] = field(default_factory=list)


def get_parent_breadcrumbs(
    node_ids: list[str], task: TaskAbstract
) -> dict[NodeId, Breadcrumb]:
    query = """
        FOR edge IN @@mainEdgeCollection
            FILTER edge.connection_type == "p_id"
            FILTER edge._from IN @nodeIds
            FOR node IN @@mainCollection
                FILTER node._id == edge._to
                RETURN {"child_id": edge._from, "child_key": node._key, "parent_breadcrumbs": node.breadcrumbs}
    """
    binds = {
        "@mainEdgeCollection": task.main_edge_collection.name,
        "nodeIds": node_ids,
        "@mainCollection": task.main_collection.name,
    }
    result_dict: dict[NodeId, Breadcrumb] = {}
    for response in task.database.aql.execute(query=query, bind_vars=binds):
        result_dict[response["child_id"]] = (
            f"{response['parent_breadcrumbs'] if response['parent_breadcrumbs'] else '/'}{response['child_key']}/"
        )

    return result_dict


def add_breadcrumbs_to_nodes(
    task: TaskAbstract,
    nodes: list[DbMoNode],
) -> list[DbMoNode]:
    node_ids: list[str] = [node.id for node in nodes]
    parent_breadcrumbs = get_parent_breadcrumbs(node_ids=node_ids, task=task)
    for node in nodes:
        breadcrumbs = parent_breadcrumbs.get(node.id, "/")
        node.breadcrumbs = breadcrumbs
    return nodes


def create_tmo_tree(task: TaskAbstract) -> list[TmoFilter]:
    task = TmoTask(key=task.key, graph_db=task.graph_db)
    task_response: TmoConfigResponse = task.execute()

    filters_dict: dict[str, TmoFilter] = {}
    for node in task_response.nodes:
        tmo_filter: TmoFilter = TmoFilter(
            tmo_id=int(node.key),
            tprm_id=node.params[0].id if node.is_grouped else None,
        )
        filters_dict[node.key] = tmo_filter
    for edge in filter(lambda x: x.link_type == "p_id", task_response.edges):
        from_filter = filters_dict[edge.target]
        to_filter = filters_dict[edge.source]
        if from_filter.tmo_id == to_filter.tmo_id:
            continue
        from_filter.parent = to_filter
        to_filter.children.append(from_filter)
    top_level_nodes = [i for i in filters_dict.values() if not i.parent]
    return top_level_nodes


def get_mo_iterator_from_tmo_filter(
    task: TaskAbstract, tmo_filter: TmoFilter
) -> Iterator[list[DbMoNode]]:
    limit = QUERY_ITEMS_LIMIT
    offset = 0
    query = """
        FOR node IN @@mainCollection
            FILTER node.tmo == @tmoId
            FILTER node.grouped_by == @tprmId
            LIMIT @offset, @limit
            RETURN node
    """
    binds = {
        "@mainCollection": task.main_collection.name,
        "tmoId": tmo_filter.tmo_id,
        "tprmId": tmo_filter.tprm_id,
        "limit": limit,
    }
    last_response_size = limit
    while limit <= last_response_size:
        binds["offset"] = offset
        chunk = [
            DbMoNode.model_validate(i)
            for i in task.database.aql.execute(query=query, bind_vars=binds)
        ]
        last_response_size = len(chunk)
        offset += last_response_size
        yield chunk


def update_nodes_chunk(nodes: list[DbMoNode], task: TaskAbstract):
    task.main_collection.update_many(
        [i.model_dump(mode="json", by_alias=True) for i in nodes],
        raise_on_document_error=True,
    )


def walk_on_a_tree(
    task: TaskAbstract, tmo_tree: list[TmoFilter], is_recursive: bool
):
    for tmo_filter in tmo_tree:
        for nodes_chunk in get_mo_iterator_from_tmo_filter(
            task=task, tmo_filter=tmo_filter
        ):
            if not nodes_chunk:
                continue
            nodes_chunk = add_breadcrumbs_to_nodes(task=task, nodes=nodes_chunk)
            update_nodes_chunk(nodes=nodes_chunk, task=task)
        if is_recursive:
            walk_on_a_tree(
                task=task,
                tmo_tree=tmo_filter.children,
                is_recursive=is_recursive,
            )


def add_breadcrumbs(task: TaskAbstract):
    tmo_tree = create_tmo_tree(task=task)
    walk_on_a_tree(task=task, tmo_tree=tmo_tree, is_recursive=True)
