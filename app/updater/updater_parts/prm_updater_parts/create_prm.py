from services.inventory import InventoryInterface
from task.building_helpers.connect_service_by_lines_update import (
    connect_service_by_lines_update,
)
from task.building_helpers.spread_connections import spread_connections
from task.helpers.convert_prms import update_prm
from task.models.dto import DbMoEdge, DbMoNode, DbTmoNode, PrmDto
from task.models.incoming_data import PRM
from task.task_abstract import TaskAbstract
from updater.updater_parts.helpers.create_mo_links import create_mo_links
from updater.updater_parts.helpers.delete_grouping_node import (
    delete_grouping_node,
)
from updater.updater_parts.helpers.find_node_by_mo_id import find_node_by_mo_id
from updater.updater_parts.helpers.find_nodes_by_mo_ids import (
    find_nodes_by_mo_ids,
)
from updater.updater_parts.helpers.find_or_create_group_node import (
    find_or_create_group_node,
)
from updater.updater_parts.helpers.get_enabled_mo_link_tprm_ids import (
    get_enabled_mo_link_tprm_ids,
)
from updater.updater_parts.helpers.get_enabled_tmo_nodes_by_tmo_ids import (
    get_enabled_tmo_nodes_by_tmo_ids,
)
from updater.updater_parts.helpers.get_groups import get_groups
from updater.updater_parts.helpers.get_tprms_dict import get_tprms_dict
from updater.updater_parts.helpers.group_node import GroupNode
from updater.updater_parts.helpers.update_node import update_node
from updater.updater_parts.mo_updater_parts.create_mo import (
    connect_nodes_as_child_chains,
    create_path_links,
)
from updater.updater_parts.mo_updater_parts.models import OperationResponse
from updater.updater_parts.prm_updater_parts.update_prm import (
    update_breadcrumbs,
)


def check_prm_exist(
    item: PRM, response: OperationResponse, db_node: DbMoNode
) -> bool:
    is_break = False
    for db_prm in db_node.data.params:
        if db_prm.id == item.id:
            response.update.append(item)
            is_break = True
            break
    return is_break


def group_node(
    db_mo_node: DbMoNode,
    item: PrmDto,
    group_dict: dict[int, list[int]],
    task: TaskAbstract,
):
    group_tprms = group_dict.get(db_mo_node.tmo, None)
    if not group_tprms:
        return
    if item.tprm_id not in group_tprms:
        return
    # old group
    old_prm = None
    if db_mo_node.data and db_mo_node.data.params:
        for param in db_mo_node.data.params:
            if param.tprm_id not in group_tprms:
                continue
            old_prm = param
            break
    if old_prm:
        old_group = find_or_create_group_node(
            node=db_mo_node,
            task=task,
            prm=old_prm,
            parent_mo_id=db_mo_node.data.p_id,
        )
        delete_grouping_node(
            group_node=old_group.node, task=task, node=db_mo_node
        )

    # new group
    item_index = group_tprms.index(item.tprm_id)
    group_db_node: GroupNode = find_or_create_group_node(
        prm=item, node=db_mo_node, task=task, parent_mo_id=db_mo_node.data.p_id
    )
    if not group_db_node.node:
        return

    db_node: DbMoNode = group_db_node.node
    nodes_links: list[DbMoNode] = [db_node]

    # parent
    if group_db_node.is_new:
        if item_index != 0:
            parent_tprm = group_tprms[item_index - 1]
            prm = [
                i for i in db_mo_node.data.params if i.tprm_id == parent_tprm
            ]
            if prm:
                prm = prm[0]
                parent_db_node = find_or_create_group_node(
                    prm=prm,
                    node=db_mo_node,
                    task=task,
                    parent_mo_id=db_mo_node.data.p_id,
                )
                if parent_db_node.node:
                    nodes_links.insert(0, parent_db_node.node)
        elif db_mo_node.data.p_id:
            parent_db_node = find_node_by_mo_id(
                task=task, mo_id=db_mo_node.data.p_id
            )
            if parent_db_node:
                nodes_links.insert(0, parent_db_node)

    # children
    if item_index + 1 == len(group_tprms):
        nodes_links.append(db_mo_node)
    else:
        child_tprm = group_tprms[item_index + 1]
        prm = [i for i in db_mo_node.data.params if i.tprm_id == child_tprm]
        if prm:
            prm = prm[0]
            child_db_node = find_or_create_group_node(
                prm=prm,
                node=db_mo_node,
                task=task,
                parent_mo_id=db_mo_node.data.p_id,
            )
            if child_db_node.node:
                nodes_links.append(child_db_node.node)

    connect_nodes_as_child_chains(task=task, nodes=nodes_links)

    parent_db_node = nodes_links[0] if nodes_links[0] != db_mo_node else None
    update_breadcrumbs(parent_node=parent_db_node, task=task, db_item=db_node)


def create_prm(
    task: TaskAbstract, items: list[PRM], inventory: InventoryInterface
) -> OperationResponse:
    response = OperationResponse()
    trace_tmo_id = task.trace_tmo_id
    group_dict = get_groups(task=task)

    unique_mo_ids = set(i.mo_id for i in items)
    mos = find_nodes_by_mo_ids(task=task, mo_ids=list(unique_mo_ids))
    mos_dict = {}
    unique_tmo_ids = set()
    for mo in mos:
        mos_dict[mo.data.id] = mo
        unique_tmo_ids.add(mo.data.tmo_id)
    enabled_tmo_nodes_by_tmo_ids: dict[int, DbTmoNode] = (
        get_enabled_tmo_nodes_by_tmo_ids(
            task=task, tmo_ids=list(unique_tmo_ids)
        )
    )

    if not enabled_tmo_nodes_by_tmo_ids:
        return response
    enabled_mo_link_tprm_ids: dict[int, set[int]] = (
        get_enabled_mo_link_tprm_ids(
            task=task, tmos_dict=enabled_tmo_nodes_by_tmo_ids
        )
    )

    tprms_dict = get_tprms_dict(task=task, items=items)

    links_to_spread: list[DbMoEdge] = []
    node_ids_for_path = []
    for item in items:  # type: PRM
        tprm = tprms_dict.get(item.tprm_id, None)
        if not tprm:
            continue
        prm_dto = PrmDto.model_validate(
            item.model_dump(mode="json", by_alias=True)
        )
        prm_dto = update_prm(prm=prm_dto, tprm=tprm, inventory=inventory)
        mo_node: DbMoNode = mos_dict.get(item.mo_id)
        if not mo_node:
            continue
        tmo_node = enabled_tmo_nodes_by_tmo_ids.get(mo_node.tmo, None)
        if tmo_node is None:
            continue
        is_break = check_prm_exist(
            item=item, response=response, db_node=mo_node
        )
        if is_break:
            continue
        new_mo_links = create_mo_links(
            task=task,
            item=item,
            db_mo_node=mo_node,
            mo_link_tprm_ids=enabled_mo_link_tprm_ids,
            trace_tmo_id=trace_tmo_id,
        )
        if new_mo_links:
            links_to_spread.extend(new_mo_links)
            node_ids_for_path.append(mo_node.id)

        group_node(
            db_mo_node=mo_node, item=prm_dto, group_dict=group_dict, task=task
        )
        mo_node = update_node(
            task=task,
            db_mo_node=mo_node,
            item=item,
            tprms_dict=tprms_dict,
            inventory=inventory,
        )
        mos_dict[item.mo_id] = mo_node

    if links_to_spread:
        spread_connections(task=task, edges=links_to_spread)
        connect_service_by_lines_update(task=task, edges=links_to_spread)
    if node_ids_for_path:
        create_path_links(task=task, node_ids=node_ids_for_path)
    return response
