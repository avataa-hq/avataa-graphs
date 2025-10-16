from task.models.dto import DbMoNode, DbTmoNode
from task.models.incoming_data import MO
from task.task_abstract import TaskAbstract
from updater.updater_parts.mo_updater_parts.models import OperationResponse


def find_mo_nodes(task: TaskAbstract, items: list[MO]) -> list[DbMoNode]:
    mo_ids = list(set(i.id for i in items))
    query = """
        FOR node IN @@mainCollection
            FILTER node.data.id IN @moIds
            RETURN node
    """
    binds = {"@mainCollection": task.main_collection.name, "moIds": mo_ids}
    response = task.database.aql.execute(query=query, bind_vars=binds)
    results = [DbMoNode.model_validate(i) for i in response]
    return results


def find_tmo_nodes(task: TaskAbstract, items: list[MO]) -> list[DbTmoNode]:
    tmo_ids = list(set(i.tmo_id for i in items))
    query = """
        FOR node IN @@tmoCollection
            FILTER node.id IN @tmoIds
            RETURN node
    """
    binds = {"@tmoCollection": task.tmo_collection.name, "tmoIds": tmo_ids}
    response = task.database.aql.execute(query=query, bind_vars=binds)
    results = [DbTmoNode.model_validate(i) for i in response]
    return results


def delete_mo_links(
    task: TaskAbstract,
    items: list[DbMoNode],
    tmo_nodes_dict: dict[int, DbTmoNode],
) -> None:
    mo_link_tprms = set()
    for tmo_node in tmo_nodes_dict.values():
        if not tmo_node.params:
            continue
        for param in tmo_node.params:
            if param.val_type != "mo_link":
                continue
            mo_link_tprms.add(param.id)
    if not mo_link_tprms:
        return

    mo_link_prm_ids = []
    for item in items:
        if not item.data.params:
            continue
        for param in item.data.params:
            if param.tprm_id not in mo_link_tprms:
                continue
            mo_link_tprms.add(param.id)
    if not mo_link_tprms:
        return

    query = """
        FOR edge IN @@mainEdgeCollection
            FILTER edge.connection_type == "mo_link"
            FILTER NOT_NULL(edge.prm)
            FILTER LENGTH(INTERSECTION(edge.prm, @moLinkPrmIds)) > 0
            REMOVE edge._key IN @@mainEdgeCollection
    """
    binds = {
        "@mainEdgeCollection": task.main_edge_collection.name,
        "moLinkPrmIds": mo_link_prm_ids,
    }
    task.database.aql.execute(query=query, bind_vars=binds)


def delete_links_geometry_line_links(
    task: TaskAbstract,
    items: list[DbMoNode],
    tmo_nodes_dict: dict[int, DbTmoNode],
) -> None:
    line_tmo_ids = set(
        i.tmo_id for i in tmo_nodes_dict.values() if i.geometry_type == "line"
    )
    if not line_tmo_ids:
        return

    line_node_ids = [i.id for i in items if i.tmo in line_tmo_ids]
    if not line_tmo_ids:
        return

    query = """
        FOR edge IN @@mainEdgeCollection
            FILTER edge.connection_type == "geometry_line"
            FILTER edge.source_id IN @nodeIds
            REMOVE edge._key IN @@mainEdgeCollection
    """
    binds = {
        "@mainEdgeCollection": task.main_edge_collection.name,
        "nodeIds": line_node_ids,
    }
    task.database.aql.execute(query=query, bind_vars=binds)


def delete_other_links(task: TaskAbstract, items: list[DbMoNode]) -> None:
    node_ids = [i.id for i in items]
    if not node_ids:
        return

    query = """
        FOR edge IN @@mainEdgeCollection
            FILTER edge._from IN @nodeIds OR edge._to IN @nodeIds
            REMOVE edge._key IN @@mainEdgeCollection
    """
    binds = {
        "@mainEdgeCollection": task.main_edge_collection.name,
        "nodeIds": node_ids,
    }
    task.database.aql.execute(query=query, bind_vars=binds)


def delete_nodes_from_groups(
    task: TaskAbstract,
    items: list[DbMoNode],
    tmo_nodes_dict: dict[int, DbTmoNode],
) -> None:
    group_tprms = task.group_by_tprm_ids
    if not group_tprms:
        return
    group_tprms = set(group_tprms)

    tmo_ids = set()
    for tmo_node in tmo_nodes_dict.values():
        if not tmo_node.params:
            continue
        item_tprm_ids = set(tprm.id for tprm in tmo_node.params)
        if item_tprm_ids.intersection(group_tprms):
            tmo_ids.add(tmo_node.tmo_id)

    mo_ids = []
    for item in items:
        if not item.data:
            continue
        if not item.data.params:
            continue
        if item.tmo not in tmo_ids:
            continue

        item_tprm_ids = set(param.tprm_id for param in item.data.params)
        if item_tprm_ids.intersection(group_tprms):
            mo_ids.append(item.id)
    if not mo_ids:
        return

    query = """
        FOR node IN @@mainCollection
            FILTER node.grouped_by_tprm IN @groupTprms
            FILTER LENGTH(INTERSECTION(node.mo_ids, @moIds)) > 0
            RETURN node
    """
    binds = {
        "@mainCollection": task.main_collection.name,
        "moIds": mo_ids,
        "groupTprms": list(group_tprms),
    }
    response_raw = task.database.aql.execute(query=query, bind_vars=binds)
    response = [DbMoNode.model_validate(i) for i in response_raw]

    to_delete_node_ids: list[dict] = []
    to_update_groups: list[dict] = []
    for node in response:
        mo_ids = set(node.mo_ids).difference(mo_ids)
        if not mo_ids:
            to_delete_node_ids.append({"_id": node.id})
        else:
            node.mo_ids = list(mo_ids)
            to_update_groups.append(node.model_dump(mode="json", by_alias=True))

    if to_delete_node_ids:
        delete_edges_query = """
            FOR edge IN @@mainEdgeCollection
                FILTER edge._from IN @nodeIds OR edge._to IN @nodeIds
                REMOVE edge._key IN @@mainEdgeCollection
        """
        delete_edges_binds = {
            "@mainEdgeCollection": task.main_edge_collection.name,
            "nodeIds": [i["_id"] for i in to_delete_node_ids],
        }
        task.database.aql.execute(
            query=delete_edges_query, bind_vars=delete_edges_binds
        )

        task.main_collection.delete_many(to_delete_node_ids)

    if to_update_groups:
        task.main_collection.update_many(to_update_groups)


def delete_nodes(task: TaskAbstract, items: list[DbMoNode]) -> None:
    node_ids = [i.data.id for i in items if i.data]
    if not node_ids:
        return

    query = """
        FOR node IN @@mainCollection
            FILTER node.data.id IN @nodeIds
            REMOVE node._key IN @@mainCollection
    """
    binds = {"@mainCollection": task.main_collection.name, "nodeIds": node_ids}
    task.database.aql.execute(query=query, bind_vars=binds)


def delete_path_links(task: TaskAbstract, items: list[DbMoNode]):
    query = """
    FOR edge IN @@pathEdgeCollection
        FILTER edge._from IN @nodeIds OR edge._to IN @nodeIds
        REMOVE edge._key IN @@pathEdgeCollection
    """
    binds = {
        "@pathEdgeCollection": task.main_path_collection.name,
        "nodeIds": [i.id for i in items],
    }
    task.database.aql.execute(query=query, bind_vars=binds)


def mo_delete(task: TaskAbstract, items: list[MO]) -> OperationResponse:
    result = OperationResponse()
    db_nodes = find_mo_nodes(items=items, task=task)
    if not db_nodes:
        return result
    tmo_nodes = find_tmo_nodes(task=task, items=items)
    tmo_nodes_dict = {i.tmo_id: i for i in tmo_nodes}

    delete_mo_links(task=task, items=db_nodes, tmo_nodes_dict=tmo_nodes_dict)
    delete_links_geometry_line_links(
        task=task, items=db_nodes, tmo_nodes_dict=tmo_nodes_dict
    )
    delete_other_links(task=task, items=db_nodes)
    delete_path_links(task=task, items=db_nodes)

    delete_nodes_from_groups(
        task=task, items=db_nodes, tmo_nodes_dict=tmo_nodes_dict
    )
    delete_nodes(task=task, items=db_nodes)

    return result
