from itertools import pairwise

from arango import DocumentInsertError

from services.inventory import InventoryInterface
from task.building_helpers.connect_service_by_lines_update import (
    connect_service_by_lines_update,
)
from task.building_helpers.spread_connections import spread_connections
from task.helpers.convert_prms import update_prm as update_prm_dto
from task.models.dto import DbMoEdge, DbMoNode, DbTmoNode, MoEdge, PrmDto
from task.models.enums import ConnectionType
from task.models.errors import ValidationError
from task.models.incoming_data import PRM
from task.task_abstract import TaskAbstract
from updater.updater_parts.helpers.create_mo_links import create_mo_links
from updater.updater_parts.helpers.delete_group_link import delete_group_link
from updater.updater_parts.helpers.find_group_node import find_group_node
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
from updater.updater_parts.helpers.get_tprms_dict import get_tprms_dict
from updater.updater_parts.helpers.group_node import GroupNode
from updater.updater_parts.helpers.update_node import update_node
from updater.updater_parts.mo_updater_parts.create_mo import (
    create_path_links,
    find_node_by_mo_id,
    get_groups,
)
from updater.updater_parts.mo_updater_parts.models import OperationResponse


def check_prm_exist(
    item: PRM, response: OperationResponse, db_node: DbMoNode
) -> tuple[PrmDto | None, bool]:
    is_break = False
    prm_dto: PrmDto | None = None
    for db_prm in db_node.data.params:
        if db_prm.id == item.id:
            prm_dto = db_prm
            break
    else:
        is_break = True
        response.create.append(item)
    return prm_dto, is_break


def update_triggers_data(
    task: TaskAbstract, prm_dto: PrmDto, response: OperationResponse
):
    query = """
        FOR node IN @@mainCollection
            FILTER node.data.id == @moId
            FILTER NOT_NULL(node.data.params)
            FOR param IN node.data.params
                FILTER @prmId IN param.parsed_value.triggers.prms
                RETURN param
    """
    binds = {
        "@mainCollection": task.main_collection.name,
        "moId": prm_dto.mo_id,
        "prmId": prm_dto.id,
    }
    db_response = task.database.aql.execute(query=query, bind_vars=binds)
    response_prms_dto = [PrmDto.model_validate(i) for i in db_response]
    response.update.extend(response_prms_dto)


def group_data(
    prm_dto: PrmDto,
    db_node: DbMoNode,
    group_tprms: list[int],
    task: TaskAbstract,
    is_start: bool,
):
    if not group_tprms:
        return [GroupNode(db_node, True)]

    old_group_node = find_group_node(
        task=task, tprm_id=prm_dto.tprm_id, real_mo_node=db_node
    )
    if old_group_node:
        delete_group_link(
            task=task, group_node=old_group_node, db_mo_node=db_node
        )

    group_parents = []
    prms_by_tprm_id_dict = {i.tprm_id: i for i in db_node.data.params}
    for group_tprm_id in group_tprms:
        if group_tprm_id == prm_dto.tprm_id:
            prm = prm_dto
        else:
            prm = prms_by_tprm_id_dict.get(group_tprm_id, None)
        if not prm:
            continue
        group_node = find_or_create_group_node(
            prm=prm, node=db_node, task=task, parent_mo_id=db_node.data.p_id
        )
        group_parents.append(group_node)
    group_parents.append(GroupNode(db_node, True))

    if len(group_parents) >= 2:
        new_links = []
        for pair in pairwise(group_parents):  # type: tuple[GroupNode, GroupNode]
            if not all(map(lambda x: x.node, pair)):
                continue
            parent, child = pair
            if not child.is_new:
                continue
            new_link = MoEdge(
                _from=child.node.id,
                _to=parent.node.id,
                connection_type=ConnectionType.P_ID,
                is_trace=False,
                virtual=False,
            )
            new_links.append(new_link)

        if new_links:
            response = task.main_edge_collection.insert_many(
                [i.model_dump(mode="json", by_alias=True) for i in new_links]
            )
            for i in response:
                if isinstance(i, DocumentInsertError):
                    raise ValidationError("Cannot create edge " + str(i))

    top_parent = group_parents[0]
    if top_parent.is_new and not is_start:
        parent_node = find_node_by_mo_id(task=task, mo_id=db_node.data.p_id)
        if parent_node:
            new_link = MoEdge(
                _from=top_parent.node.id,
                _to=parent_node.id,
                connection_type=ConnectionType.P_ID,
                is_trace=False,
                virtual=False,
            )
            response = task.main_edge_collection.insert(
                new_link.model_dump(mode="json", by_alias=True), return_new=True
            )
            if isinstance(response, DocumentInsertError):
                raise ValidationError("Cannot create edge " + str(response))

    parent_node = top_parent.node if top_parent.node.id != db_node.id else None
    update_breadcrumbs(parent_node=parent_node, db_item=db_node, task=task)


def update_breadcrumbs(
    parent_node: DbMoNode | None, db_item: DbMoNode, task: TaskAbstract
):
    # breadcrumbs
    node_breadcrumbs = parent_node.breadcrumbs if parent_node else "/"
    old_breadcrumbs = f"{db_item.breadcrumbs}{db_item.key}"
    new_breadcrumbs = f"{node_breadcrumbs}{db_item.key}"
    db_item.breadcrumbs = node_breadcrumbs

    replace_child_breadcrumbs_query = """
               FOR node IN @@mainCollection
                   FILTER STARTS_WITH(node.breadcrumbs, [@parentBreadcrumbs])
                   UPDATE node._key
                       WITH {"breadcrumbs": SUBSTITUTE(node.breadcrumbs, @parentBreadcrumbs, @newParentBreadcrumbs, 1)}
                       IN @@mainCollection
           """
    replace_child_breadcrumbs_binds = {
        "@mainCollection": task.main_collection.name,
        "parentBreadcrumbs": old_breadcrumbs,
        "newParentBreadcrumbs": new_breadcrumbs,
    }
    task.database.aql.execute(
        query=replace_child_breadcrumbs_query,
        bind_vars=replace_child_breadcrumbs_binds,
    )


def update_mo_link(
    prm_dto: PrmDto,
    old_prm_dto: PrmDto,
    db_node: DbMoNode,
    task: TaskAbstract,
    mo_link_tprm_ids: dict[int, set[int]],
    trace_tmo_id: int | None,
) -> list[DbMoEdge]:
    results = []
    if prm_dto.tprm_id not in mo_link_tprm_ids.get(db_node.tmo, set()):
        return results
    new_values = (
        set(prm_dto.value)
        if isinstance(prm_dto.value, list)
        else {prm_dto.value}
    )
    old_values = (
        set(old_prm_dto.value)
        if isinstance(old_prm_dto.value, list)
        else {old_prm_dto.value}
    )
    if old_values == new_values:
        return results

    # delete old links in main edge collection
    query = """
        FOR edge IN @@mainEdgeCollection
            FILTER @prmId IN edge.prm
            REMOVE edge._key IN @@mainEdgeCollection
    """
    binds = {
        "@mainEdgeCollection": task.main_edge_collection.name,
        "prmId": old_prm_dto.id,
    }
    task.database.aql.execute(query=query, bind_vars=binds)

    # create new
    return create_mo_links(
        task=task,
        item=prm_dto,
        db_mo_node=db_node,
        mo_link_tprm_ids=mo_link_tprm_ids,
        trace_tmo_id=trace_tmo_id,
    )


def update_prm(
    task: TaskAbstract, items: list[PRM], inventory: InventoryInterface
) -> OperationResponse:
    response = OperationResponse()
    trace_tmo_id = task.trace_tmo_id
    group_dict = get_groups(task=task)
    start_from_tmo = task.start_from_tmo

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
    enabled_mo_link_tprm_ids: dict[int, set[int]] = (
        get_enabled_mo_link_tprm_ids(
            task=task, tmos_dict=enabled_tmo_nodes_by_tmo_ids
        )
    )

    tprms_dict = get_tprms_dict(task=task, items=items)

    links_to_spread: list[DbMoEdge] = []
    node_ids_for_path = []
    for item in items:
        tprm = tprms_dict.get(item.tprm_id, None)
        prm_dto = PrmDto.model_validate(
            item.model_dump(mode="dict", by_alias=True)
        )
        prm_dto = update_prm_dto(prm=prm_dto, tprm=tprm, inventory=inventory)

        update_triggers_data(
            task=task,
            prm_dto=prm_dto,
            response=response,
        )
        mo_node = mos_dict.get(item.mo_id)
        if not mo_node:
            continue
        is_start = mo_node.tmo == start_from_tmo if start_from_tmo else False
        tmo_node = enabled_tmo_nodes_by_tmo_ids.get(mo_node.tmo, None)
        if tmo_node is None:
            continue
        old_prm_dto, is_break = check_prm_exist(
            item=item, response=response, db_node=mo_node
        )
        if is_break:
            continue
        group_tprms = group_dict.get(tmo_node.tmo_id, set())
        group_data(
            prm_dto=prm_dto,
            db_node=mo_node,
            task=task,
            group_tprms=group_tprms,
            is_start=is_start,
        )
        mo_links = update_mo_link(
            prm_dto=prm_dto,
            old_prm_dto=old_prm_dto,
            db_node=mo_node,
            task=task,
            mo_link_tprm_ids=enabled_mo_link_tprm_ids,
            trace_tmo_id=trace_tmo_id,
        )
        if mo_links:
            node_ids_for_path.append(mo_node.id)
        links_to_spread.extend(mo_links)

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
