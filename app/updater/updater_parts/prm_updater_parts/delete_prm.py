from collections import defaultdict

from task.models.dto import DbMoEdge
from task.models.incoming_data import PRM, TPRM
from task.task_abstract import TaskAbstract
from updater.updater_parts.helpers.delete_group_link import delete_group_link
from updater.updater_parts.helpers.delete_grouping_node import (
    delete_grouping_node,
)
from updater.updater_parts.helpers.find_group_node import find_group_node
from updater.updater_parts.helpers.find_nodes_by_mo_ids import (
    find_nodes_by_mo_ids,
)
from updater.updater_parts.helpers.get_groups import get_groups
from updater.updater_parts.helpers.get_tprms_dict import get_tprms_dict
from updater.updater_parts.mo_updater_parts.models import OperationResponse
from updater.updater_parts.mo_updater_parts.update_mo import (
    refresh_path_collection,
)
from updater.updater_parts.prm_updater_parts.update_prm import (
    update_breadcrumbs,
)


def delete_old_prm_links(
    task: TaskAbstract, items: list[PRM], tprms_dict: dict[int, TPRM]
):
    if not tprms_dict:
        return
    mo_link_tprm_ids = [
        i.id for i in tprms_dict.values() if i.val_type == "mo_link"
    ]
    prm_ids = []
    mo_ids = set()
    for item in items:
        if item.tprm_id in mo_link_tprm_ids:
            prm_ids.append(item.id)
            mo_ids.add(item.mo_id)
    if not prm_ids:
        return

    # delete old links in main edge collection
    query = """
        FOR edge IN @@mainEdgeCollection
            FILTER LENGTH(INTERSECTION(edge.prm, @prmIds)) > 0
            RETURN edge
    """
    binds = {
        "@mainEdgeCollection": task.main_edge_collection.name,
        "prmIds": prm_ids,
    }
    response = task.database.aql.execute(query=query, bind_vars=binds)

    edge_ids_to_delete: list[str] = []
    edges_to_update: list[DbMoEdge] = []
    for edge_raw in response:
        db_edge = DbMoEdge.model_validate(edge_raw)
        edge_prms = set(db_edge.prm)
        edge_prms = edge_prms.difference(prm_ids)
        if edge_prms:
            db_edge.prm = list(edge_prms)
            edges_to_update.append(db_edge)
        else:
            edge_ids_to_delete.append(db_edge.id)
    if edges_to_update:
        task.main_edge_collection.update_many(
            [i.model_dump(mode="json", by_alias=True) for i in edges_to_update]
        )
    if edge_ids_to_delete:
        task.main_edge_collection.delete_many(
            [{"_id": i} for i in edge_ids_to_delete]
        )

    # recalculate path collection
    db_items = find_nodes_by_mo_ids(task=task, mo_ids=list(mo_ids))
    refresh_path_collection(db_items=db_items, task=task)


def delete_prms_from_mos(task: TaskAbstract, items: list[PRM]):
    prm_ids_by_mo_ids_dict = defaultdict(set)
    for item in items:
        prm_ids_by_mo_ids_dict[item.mo_id].add(item.id)
    db_items = find_nodes_by_mo_ids(
        task=task, mo_ids=list(prm_ids_by_mo_ids_dict)
    )
    for db_item in db_items:
        prm_ids_to_delete = prm_ids_by_mo_ids_dict.get(db_item.mo_id, None)
        if prm_ids_to_delete:
            continue
        if not db_item.data.params:
            continue
        db_item.data.params = [
            i for i in db_item.data.params if i.id not in prm_ids_to_delete
        ]
    task.main_collection.update_many(
        [i.model_dump(mode="json", by_alias=True) for i in db_items]
    )


def delete_group_by_prms(task: TaskAbstract, items: list[PRM]):
    unique_mo_ids = set(i.mo_id for i in items)
    mos = find_nodes_by_mo_ids(task=task, mo_ids=list(unique_mo_ids))
    mos_dict = {i.data.id: i for i in mos}
    group_tprms_by_tmo_dict = get_groups(task=task)

    nodes_to_update = []
    for item in items:
        mo_node = mos_dict.get(item.mo_id, None)
        if not mo_node:
            continue
        tprms_by_tmo = group_tprms_by_tmo_dict.get(mo_node.tmo, None)
        if not tprms_by_tmo:
            continue
        if item.tprm_id not in tprms_by_tmo:
            continue
        group_mo_node = find_group_node(
            task=task, tprm_id=item.tprm_id, real_mo_node=mo_node
        )
        if not group_mo_node:
            continue
        delete_group_link(
            task=task, db_mo_node=mo_node, group_node=group_mo_node
        )
        delete_grouping_node(task=task, group_node=group_mo_node, node=mo_node)
        update_breadcrumbs(task=task, parent_node=None, db_item=mo_node)
        nodes_to_update.append(mo_node)
    if nodes_to_update:
        task.main_collection.update_many(
            [i.model_dump(by_alias=True, mode="json") for i in nodes_to_update]
        )


def update_mos(task: TaskAbstract, items: list[PRM]):
    mo_ids = set()
    prm_ids = set()
    for item in items:
        mo_ids.add(item.mo_id)
        prm_ids.add(item.id)

    mos = find_nodes_by_mo_ids(mo_ids=list(mo_ids), task=task)
    mos_for_update = []
    for mo in mos:
        params = mo.data.params
        len_before = len(params)
        params = [i for i in params if i.id not in prm_ids]
        len_after = len(params)
        if len_before != len_after:
            mo.data.params = params
            mos_for_update.append(mo)
    task.main_collection.update_many(
        [i.model_dump(mode="json", by_alias=True) for i in mos_for_update]
    )


def delete_prm(task: TaskAbstract, items: list[PRM]) -> OperationResponse:
    response = OperationResponse()
    tprms_dict = get_tprms_dict(task=task, items=items)
    delete_old_prm_links(task=task, items=items, tprms_dict=tprms_dict)
    delete_group_by_prms(task=task, items=items)
    update_mos(task=task, items=items)
    return response
