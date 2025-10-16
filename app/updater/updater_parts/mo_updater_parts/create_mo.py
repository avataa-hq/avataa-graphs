from collections import defaultdict
from itertools import pairwise
from typing import Literal

from arango import DocumentInsertError

from services.inventory import InventoryInterface
from task.building_helpers.connect_service_by_lines_update import (
    connect_service_by_lines_update,
)
from task.building_helpers.fill_path_edge_collection import UniqueFromToEdge
from task.building_helpers.spread_connections import spread_connections
from task.helpers.convert_prms import update_prm
from task.models.dto import DbMoEdge, DbMoNode, DbTmoEdge, MoEdge, MoNode
from task.models.enums import ConnectionType
from task.models.errors import ValidationError
from task.models.incoming_data import MO, TPRM
from task.task_abstract import TaskAbstract
from updater.updater_parts.helpers.find_children_iterator import (
    find_children_iterator,
)
from updater.updater_parts.helpers.find_node_by_mo_id import find_node_by_mo_id
from updater.updater_parts.helpers.find_or_create_group_node import (
    find_or_create_group_node,
)
from updater.updater_parts.helpers.get_groups import get_groups
from updater.updater_parts.helpers.get_line_tmo_ids import get_line_tmo_ids
from updater.updater_parts.helpers.group_node import GroupNode
from updater.updater_parts.mo_updater_parts.models import OperationResponse


def connect_nodes_as_child_chains(task: TaskAbstract, nodes: list[DbMoNode]):
    """Соединяем ноды в цепочку. Родитель первый"""
    """Сначала ищем уже существующие связи"""
    if len(nodes) < 2:
        return
    node_ids = tuple(i.id for i in nodes)
    pairs = set(pairwise(node_ids))
    query = """
        FOR edge IN @@mainEdgeCollection
            FILTER edge._from IN @nodeIds
            FILTER edge._to IN @nodeIds
            FILTER edge.connection_type == "p_id"
            RETURN [edge._to, edge._from]
    """
    binds = {
        "@mainEdgeCollection": task.main_edge_collection.name,
        "nodeIds": node_ids,
    }
    for edge in task.database.aql.execute(query=query, bind_vars=binds):
        if tuple(edge) in pairs:
            pairs.remove(edge)
    new_links = []
    for pair in pairs:
        new_link = MoEdge(
            _from=pair[1],
            _to=pair[0],
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


def connect_with_children(
    task: TaskAbstract,
    node: DbMoNode,
    children_group_tprm_id: int | None,
    tmo_id: int,
):
    children_ids_query = """
        FOR node IN @@mainCollection
            FILTER node.tmo == @tmoId
            FILTER node.data.p_id == @moId
            RETURN node
    """
    children_ids_binds = {
        "@mainCollection": task.main_collection.name,
        "moId": node.data.id,
        "tmoId": tmo_id,
    }
    child_nodes = [
        DbMoNode.model_validate(i)
        for i in task.database.aql.execute(
            query=children_ids_query, bind_vars=children_ids_binds
        )
    ]
    if not child_nodes:
        return
    if children_group_tprm_id:
        children_mo_ids = [i.data.id for i in child_nodes]
        group_children_query = """
            FOR node IN @@mainCollection
                FILTER node.grouped_by_tprm == @tprmId
                FILTER LENGTH(INTERSECTION(node.mo_ids, @moIds)) > 0
                RETURN node._id
        """
        group_children_binds = {
            "@mainCollection": task.main_collection.name,
            "tprmId": children_group_tprm_id,
            "moIds": children_mo_ids,
        }
        child_nodes = [
            DbMoNode.model_validate(i)
            for i in task.database.aql.execute(
                query=group_children_query, bind_vars=group_children_binds
            )
        ]
    new_links = []
    for child_node in child_nodes:
        new_link = MoEdge(
            _from=child_node.id,
            _to=node.id,
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


def find_node(task: TaskAbstract, item: MO) -> DbMoNode | None:
    query = """
        FOR node IN @@mainCollection
            FILTER node.data.id == @moId
            LIMIT 1
            RETURN node
    """
    binds = {"@mainCollection": task.main_collection.name, "moId": item.id}
    response = list(task.database.aql.execute(query=query, bind_vars=binds))
    return response[0] if response else None


def create_node(
    item: MO,
    task: TaskAbstract,
    is_trace: bool,
    tprms_dict,
    inventory: InventoryInterface,
) -> DbMoNode:
    node = MoNode(
        name=item.name,
        label=item.label,
        tmo=item.tmo_id,
        mo_ids=[item.id],
        is_trace=is_trace,
        data=item.model_dump(by_alias=True, mode="json"),
        indexed=None,  # Заполняем батчем на след этапах как и breadcrumbs
    )
    response = task.main_collection.insert(
        node.model_dump(mode="json", by_alias=True),
        return_new=True,
        keep_none=True,
    )
    result = DbMoNode.model_validate(response["new"])
    for param in result.data.params:
        tprm = tprms_dict.get(param.tprm_id)
        update_prm(prm=param, tprm=tprm, inventory=inventory)
    return result


def create_or_none_mo_node(
    task: TaskAbstract,
    item: MO,
    is_trace: bool,
    tprms_dict: dict[int, TPRM],
    inventory: InventoryInterface,
) -> DbMoNode | None:
    db_node = find_node(task=task, item=item)
    if db_node:
        return None
    db_node = create_node(
        item=item,
        task=task,
        is_trace=is_trace,
        tprms_dict=tprms_dict,
        inventory=inventory,
    )
    return db_node


def get_tmo_children_dict(
    task: TaskAbstract,
) -> dict[int, list[int]]:  # {parent_id: list[child_id]}
    result: dict[int, list[int]] = defaultdict(list)
    query = """
        FOR edge IN @@tmoEdgeCollection
            FILTER edge.link_type == "p_id"
            RETURN edge
    """
    binds = {"@tmoEdgeCollection": task.tmo_edge_collection.name}
    for edge_raw in task.database.aql.execute(query=query, bind_vars=binds):
        edge = DbTmoEdge.model_validate(edge_raw)
        from_ = int(edge.from_.split("/")[-1])
        to_ = int(edge.to_.split("/")[-1])
        result[to_].append(from_)
    return result


def group_data_and_create_p_id_links(
    task: TaskAbstract, node: DbMoNode, group_tprms: list[int]
) -> list[GroupNode]:
    if not group_tprms:
        return [GroupNode(node, True)]
    group_parents = []
    prms_by_tprm_id_dict = {i.tprm_id: i for i in node.data.params}
    for group_tprm_id in group_tprms:
        prm = prms_by_tprm_id_dict.get(group_tprm_id, None)
        if not prm:
            continue
        group_node = find_or_create_group_node(
            prm=prm, node=node, task=task, parent_mo_id=node.data.p_id
        )
        group_parents.append(group_node)
    group_parents.append(GroupNode(node, True))

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
    return group_parents


def create_point_links(
    task: TaskAbstract, node: DbMoNode, enabled_points_tmo_ids: set[int]
) -> tuple[DbMoNode | None, DbMoNode | None]:
    def create_point_link(point_side: Literal["a", "b"], point: DbMoNode):
        if not point:
            return
        if point.tmo not in enabled_points_tmo_ids:
            return
        new_link = MoEdge(
            _from=node.id,
            _to=point.id,
            connection_type=ConnectionType.POINT_A
            if point_side == "a"
            else ConnectionType.POINT_B,
            virtual=False,
            is_trace=False,
        )
        response = task.main_edge_collection.insert(
            new_link.model_dump(mode="json", by_alias=True)
        )
        if isinstance(response, DocumentInsertError):
            raise ValidationError("Cannot create edge " + str(response))

    point_a_node = find_node_by_mo_id(task=task, mo_id=node.data.point_a_id)
    create_point_link(point_side="a", point=point_a_node)

    point_b_node = find_node_by_mo_id(task=task, mo_id=node.data.point_b_id)
    create_point_link(point_side="b", point=point_b_node)

    return point_a_node, point_b_node


def create_geometry_line_edge(
    points: tuple[DbMoNode, DbMoNode],
    node: DbMoNode,
    task: TaskAbstract,
):
    new_edge = MoEdge(
        _from=points[0].id,
        _to=points[1].id,
        is_trace=False,
        virtual=True,
        connection_type=ConnectionType.GEOMETRY_LINE,
        source_id=node.id,
    )
    response = task.main_edge_collection.insert(
        new_edge.model_dump(mode="json", by_alias=True)
    )
    if isinstance(response, DocumentInsertError):
        raise ValidationError("Cannot create edge " + str(response))


def spread_node_and_childen_links(task: TaskAbstract, nodes: list[DbMoNode]):
    get_links_query = """
        LET childrenIds = (
            FOR edge IN @@mainEdgeCollection
                FILTER edge.connection_type == "p_id"
                FILTER edge._to IN @nodeIds
                RETURN edge._from
        )
        LET allIds = APPEND(childrenIds, @nodeIds)
        FOR edge IN @@mainEdgeCollection
            FILTER edge.connection_type != "p_id"
            FILTER edge._from IN allIds OR edge._to IN allIds
            RETURN edge
    """
    get_links_binds = {
        "@mainEdgeCollection": task.main_edge_collection.name,
        "nodeIds": [i.id for i in nodes],
    }
    response = task.database.aql.execute(
        query=get_links_query, bind_vars=get_links_binds
    )
    links = [DbMoEdge.model_validate(i) for i in response]
    spread_connections(task=task, edges=links)
    connect_service_by_lines_update(task=task, edges=links)


def find_parent(task: TaskAbstract, node: DbMoNode) -> DbMoNode | None:
    query = """
        FOR v, e IN 1 OUTBOUND @nodeId
        GRAPH @mainGraph
            FILTER e.connection_type == "p_id"
            RETURN v
    """
    binds = {"nodeId": node.id, "mainGraph": task.config.graph_data_graph_name}
    response = list(task.database.aql.execute(query=query, bind_vars=binds))
    if not len(response):
        return None
    result = DbMoNode.model_validate(response[0])
    return result


def create_breadcrumbs(
    parent_node: DbMoNode | None, child_node: DbMoNode | None
) -> str:
    new_breadcrumbs = parent_node.breadcrumbs if parent_node else ""
    new_breadcrumbs = (
        f"{new_breadcrumbs}{parent_node.key}"
        if parent_node
        else new_breadcrumbs
    )
    child_breadcrumbs = child_node.breadcrumbs if child_node else "/"
    new_breadcrumbs = f"{new_breadcrumbs}{child_breadcrumbs}"
    return new_breadcrumbs


def recreate_breadcrumbs(task: TaskAbstract, group_parents: list[GroupNode]):
    first_new_index = -1
    for index, parent in enumerate(group_parents):
        if parent.is_new:
            first_new_index = index
            break

    first_node_to_change = group_parents[first_new_index]
    parent_node = find_parent(task=task, node=first_node_to_change.node)
    prev_node = parent_node

    nodes_to_update = []
    for parent in group_parents[first_new_index:]:  # type: GroupNode
        if not parent.node:
            prev_node = parent.node
            continue
        node = parent.node
        new_child_breadcrumbs = create_breadcrumbs(
            parent_node=prev_node, child_node=node
        )
        node.breadcrumbs = new_child_breadcrumbs
        nodes_to_update.append(node)
        prev_node = node
    if nodes_to_update:
        task.main_edge_collection.update_many(
            [i.model_dump(mode="json", by_alias=True) for i in nodes_to_update]
        )

    buffer = []
    buffer_size = 50
    replace_child_breadcrumbs_query = """
        FOR node IN @@mainCollection
            FILTER STARTS_WITH(node.breadcrumbs, [@parentBreadcrumbs])
            UPDATE node._key
                WITH {"breadcrumbs": SUBSTITUTE(node.breadcrumbs, @parentBreadcrumbs, @newParentBreadcrumbs, 1)}
                IN @@mainCollection
    """
    replace_child_breadcrumbs_binds = {
        "@mainCollection": task.main_collection.name
    }
    for child in find_children_iterator(task=task, node=group_parents[-1].node):
        old_breadcrumbs = create_breadcrumbs(parent_node=None, child_node=child)
        new_child_breadcrumbs = create_breadcrumbs(
            parent_node=prev_node, child_node=child
        )
        new_breadcrumbs = f"{new_child_breadcrumbs}/{child.key}"
        child.breadcrumbs = new_child_breadcrumbs
        buffer.append(child)
        if len(buffer) >= buffer_size:
            task.main_collection.update_many(
                [i.model_dump(mode="json", by_alias=True) for i in buffer]
            )
            buffer = []

        replace_child_breadcrumbs_binds["parentBreadcrumbs"] = old_breadcrumbs
        replace_child_breadcrumbs_binds["newParentBreadcrumbs"] = (
            new_breadcrumbs
        )
        task.database.aql.execute(
            query=replace_child_breadcrumbs_query,
            bind_vars=replace_child_breadcrumbs_binds,
        )

    if buffer:
        task.main_collection.update_many(
            [i.model_dump(mode="json", by_alias=True) for i in buffer]
        )


def get_node_ids_for_path_links(group_parents: list[GroupNode]) -> list[str]:
    node_ids = []
    for node in group_parents:  # type: GroupNode
        if not node.node:
            continue
        if node.node.grouped_by_tprm:
            continue
        node_ids.append(node.node.id)
    return node_ids


def create_path_links(task: TaskAbstract, node_ids: list[str]):
    if not node_ids:
        return
    query = """
        FOR edge IN @@mainEdgeCollection
            FILTER edge.virtual == false
            FILTER edge._from != edge._to
            FILTER edge._from IN @nodeIds OR edge._to IN @nodeIds
            FOR node IN @@mainCollection
                FILTER node._id == edge._to
                FILTER IS_NULL(node.grouped_by_tprm)
            RETURN {"_from": edge._from, "_to": edge._to}
    """
    binds = {
        "@mainEdgeCollection": task.main_edge_collection.name,
        "nodeIds": node_ids,
        "@mainCollection": task.main_collection.name,
    }
    main_edge_set = set()
    for edge_raw in task.database.aql.execute(query=query, bind_vars=binds):
        main_edge_set.add(UniqueFromToEdge.model_validate(edge_raw))

    query = """
        FOR edge IN @@pathEdgeCollection
            FILTER edge._from IN @nodeIds OR edge._to IN @nodeIds
            RETURN {"_from": edge._from, "_to": edge._to}
    """
    binds = {
        "@pathEdgeCollection": task.main_path_collection.name,
        "nodeIds": node_ids,
    }
    path_edge_set = set()
    for edge_raw in task.database.aql.execute(query=query, bind_vars=binds):
        path_edge_set.add(UniqueFromToEdge.model_validate(edge_raw))
    new_path_edges = main_edge_set.difference(path_edge_set)
    if new_path_edges:
        task.main_path_collection.insert_many(
            [i.model_dump(mode="json", by_alias=True) for i in new_path_edges]
        )


def create_path_p_id_link(
    task: TaskAbstract, node: DbMoNode, parent_node: DbMoNode | None
):
    if not parent_node:
        return
    query = """
        FOR edge IN @@pathEdgeCollection
            FILTER (edge._from == @fromId AND edge._to == @toId)
                OR (edge._from == @toId AND edge._to == @fromId)
            RETURN edge
    """
    binds = {
        "@pathEdgeCollection": task.main_path_collection.name,
        "fromId": node.id,
        "toId": parent_node.id,
    }
    result = list(task.database.aql.execute(query=query, bind_vars=binds))
    if not result:
        edge = UniqueFromToEdge(_from=node.id, _to=parent_node.id)
        task.main_path_collection.insert(
            edge.model_dump(mode="json", by_alias=True)
        )


def get_tprms_dict(task: TaskAbstract, items: list[MO]) -> dict[int, TPRM]:
    results = {}
    unique_tprms = set()
    unique_tmos = set()
    for item in items:
        unique_tmos.add(item.tmo_id)
        for param in item.params:
            unique_tprms.add(param.tprm_id)
    if not unique_tprms:
        return results
    query = """
        FOR node IN @@tmoCollection
            FILTER node.id IN @tmos
            FILTER NOT_NULL(node.params)
            FOR param IN node.params
                FILTER param.id IN @tprms
                RETURN param
    """
    binds = {
        "@tmoCollection": task.tmo_collection.name,
        "tmos": list(unique_tmos),
        "tprms": list(unique_tprms),
    }
    response = task.database.aql.execute(query=query, bind_vars=binds)
    for item_raw in response:
        tprm = TPRM.model_validate(item_raw)
        results[tprm.id] = tprm
    return results


def get_enabled_points_by_tmo(task: TaskAbstract) -> dict[int, set[int]]:
    query = """
        FOR node IN @@tmoCollection
            FILTER node.enabled == true
            LET otherSideTmoIds = (FOR edge IN @@tmoEdgeCollection
                FILTER edge._from == node._id
                FILTER edge.enabled == true
                FILTER edge.link_type == "point_tmo_constraint"
                FOR otherNode IN @@tmoCollection
                    FILTER otherNode._id == edge._to
                    RETURN otherNode.id)
            RETURN {"nodeId": node.id, "otherSideTmoIds": otherSideTmoIds}
    """
    binds = {
        "@tmoCollection": task.tmo_collection.name,
        "@tmoEdgeCollection": task.main_edge_collection.name,
    }
    results = defaultdict(set)
    for row in task.database.aql.execute(query=query, bind_vars=binds):
        results[row["nodeId"]].update(row["otherSideTmoIds"])
    return results


def mo_create(
    task: TaskAbstract, items: list[MO], inventory: InventoryInterface
):
    to_update_items = []
    trace_tmo_id = task.trace_tmo_id
    start_from_tmo = task.start_from_tmo
    group_dict = get_groups(task=task)
    tmo_children_dict = get_tmo_children_dict(task=task)
    line_tmo_ids = get_line_tmo_ids(task=task)
    tprms_dict = get_tprms_dict(task=task, items=items)
    enabled_points_by_tmo = get_enabled_points_by_tmo(task=task)

    db_mo_nodes = []
    for item in items:
        is_trace = item.tmo_id == trace_tmo_id if trace_tmo_id else False
        if not is_trace and item.tmo_id not in enabled_points_by_tmo:
            continue

        is_start = item.tmo_id == start_from_tmo if start_from_tmo else False
        group_tprms = group_dict.get(item.tmo_id, [None])

        db_mo_node = create_or_none_mo_node(
            task=task,
            item=item,
            is_trace=is_trace,
            tprms_dict=tprms_dict,
            inventory=inventory,
        )
        if not db_mo_node:
            to_update_items.append(item)
            continue
        db_mo_nodes.append(db_mo_node)

        # group and connect to parent
        group_parents = group_data_and_create_p_id_links(
            task=task, node=db_mo_node, group_tprms=group_tprms
        )
        top_parent = group_parents[0]
        parent_node = find_node_by_mo_id(task=task, mo_id=db_mo_node.data.p_id)
        if top_parent.is_new and not is_start and parent_node:
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

        # connect node to nearest children or group of children
        tmo_children = tmo_children_dict.get(item.tmo_id, [])
        for tmo_child in tmo_children:
            for tmo_group_tprm in group_tprms:
                connect_with_children(
                    task=task,
                    node=db_mo_node,
                    children_group_tprm_id=tmo_group_tprm,
                    tmo_id=tmo_child,
                )

        # point a, point b
        point_nodes = create_point_links(
            task=task,
            node=db_mo_node,
            enabled_points_tmo_ids=enabled_points_by_tmo.get(
                item.tmo_id, set()
            ),
        )

        # geometry type line connection
        if all(point_nodes) and item.tmo_id in line_tmo_ids:
            create_geometry_line_edge(
                points=point_nodes, node=db_mo_node, task=task
            )

        recreate_breadcrumbs(group_parents=group_parents, task=task)
        node_ids = get_node_ids_for_path_links(group_parents=group_parents)
        create_path_p_id_link(
            task=task, node=group_parents[-1].node, parent_node=parent_node
        )
        create_path_links(task=task, node_ids=node_ids)

    spread_node_and_childen_links(task=task, nodes=db_mo_nodes)

    response = OperationResponse()
    if to_update_items:
        response.update = to_update_items
    return response
