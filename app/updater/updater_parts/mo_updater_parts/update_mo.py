from arango import DocumentInsertError

from task.building_helpers.connect_service_by_lines_update import (
    connect_service_by_lines_update,
)
from task.building_helpers.fill_path_edge_collection import UniqueFromToEdge
from task.building_helpers.spread_connections import spread_connections
from task.models.dto import DbMoEdge, DbMoNode, MoEdge
from task.models.enums import ConnectionType
from task.models.errors import ValidationError
from task.models.incoming_data import MO
from task.task_abstract import TaskAbstract
from updater.updater_parts.helpers.find_children_iterator import (
    find_children_iterator,
)
from updater.updater_parts.helpers.find_node_by_mo_id import find_node_by_mo_id
from updater.updater_parts.helpers.get_line_tmo_ids import get_line_tmo_ids
from updater.updater_parts.mo_updater_parts.delete_mo import find_mo_nodes
from updater.updater_parts.mo_updater_parts.models import OperationResponse


def parent_changed(
    db_item: DbMoNode,
    item: MO,
    result: OperationResponse,
    task: TaskAbstract,
    main_tmo_id: int,
) -> tuple[list[DbMoEdge], bool]:  # bool - break
    edges_to_spread = []
    parent_node = find_node_by_mo_id(task=task, mo_id=item.p_id)
    if main_tmo_id != item.tmo_id:
        if not parent_node:
            current_node_and_children = [db_item]
            current_level = [db_item]
            while current_level:
                # collect all children
                next_level = []
                for current_item in current_level:
                    for child in find_children_iterator(
                        task=task, node=current_item
                    ):
                        next_level.append(child)
                        current_node_and_children.append(child)
                current_level = next_level

            result.delete.extend(
                [i.data for i in current_node_and_children if i.data]
            )
            return edges_to_spread, True

        find_node_links = """
            FOR edge IN @@mainEdgeCollection
                FILTER edge._from == @nodeId OR edge._to == @nodeId
                FILTER edge.connection_type != "p_id"
                RETURN edge
        """
        binds = {
            "@mainEdgeCollection": task.main_edge_collection.name,
            "nodeId": db_item.id,
        }
        source_ids = set()
        prm_ids = set()

        for edge_raw in task.database.aql.execute(
            query=find_node_links, bind_vars=binds
        ):
            edge = DbMoEdge.model_validate(edge_raw)
            if edge.source_id:
                source_ids.add(edge.source_id)
            if edge.prm:
                prm_ids.update(edge.prm)
        source_ids = list(source_ids)
        prm_ids = list(prm_ids)
        # delete old spread
        query = """
            FOR edge IN @@mainEdgeCollection
                FILTER edge.virtual == true
                FILTER edge.source_id IN @nodeIds OR edge.prm IN @prmIds
                REMOVE edge._key IN @@mainEdgeCollection
        """
        binds = {
            "@mainEdgeCollection": task.main_edge_collection.name,
            "nodeIds": source_ids,
            "prmIds": prm_ids,
        }
        task.database.aql.execute(query=query, bind_vars=binds)

        # edges to spread
        query = """
            FOR edge IN @@mainEdgeCollection
                FILTER edge.virtual == false
                FILTER edge.source_id IN @nodeIds OR edge.prm IN @prmIds
                RETURN edge
        """
        binds = {
            "@mainEdgeCollection": task.main_edge_collection.name,
            "nodeIds": source_ids,
            "prmIds": prm_ids,
        }
        edges_to_spread.extend(
            [
                DbMoEdge.model_validate(edge)
                for edge in task.database.aql.execute(
                    query=query, bind_vars=binds
                )
            ]
        )

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

    return edges_to_spread, False


def point_link_changed(
    db_item: DbMoNode, item: MO, task: TaskAbstract, line_tmo_ids: set[int]
) -> tuple[list[DbMoEdge], bool]:  # bool - break
    query_delete_point = """
            FOR edge IN @@mainEdgeCollection
                FILTER edge.connection_type == @connectionType
                FILTER edge.source_id == @nodeId
                REMOVE edge._key IN @@mainEdgeCollection
        """
    binds = {
        "@mainEdgeCollection": task.main_edge_collection.name,
        "nodeId": db_item.id,
    }

    new_edges = []

    # point a
    is_point_a_changed = item.point_a_id != db_item.data.point_a_id
    point_a_node = (
        find_node_by_mo_id(task=task, mo_id=item.point_a_id)
        if item.point_a_id
        else None
    )
    if is_point_a_changed:
        binds["connectionType"] = ConnectionType.POINT_A.value
        task.database.aql.execute(query=query_delete_point, bind_vars=binds)

        if point_a_node:
            new_edge = MoEdge(
                _from=db_item.id,
                _to=point_a_node.id,
                connection_type=ConnectionType.POINT_A,
                is_trace=False,
                virtual=False,
                source_id=db_item.id,
            )
            new_edges.append(new_edge)

    # point b
    is_point_b_changed = item.point_b_id != db_item.data.point_b_id
    point_b_node = (
        find_node_by_mo_id(task=task, mo_id=item.point_b_id)
        if item.point_b_id
        else None
    )
    if is_point_b_changed:
        binds["connectionType"] = ConnectionType.POINT_B.value
        task.database.aql.execute(query=query_delete_point, bind_vars=binds)

        if point_b_node:
            new_edge = MoEdge(
                _from=db_item.id,
                _to=point_b_node.id,
                connection_type=ConnectionType.POINT_B,
                is_trace=False,
                virtual=False,
                source_id=db_item.id,
            )
            new_edges.append(new_edge)

    if item.tmo_id in line_tmo_ids and any(
        (is_point_a_changed, is_point_b_changed)
    ):
        # delete geometry type line
        query = """
                FOR edge IN @@mainEdgeCollection
                    FILTER edge.source_id == @nodeId
                    FILTER edge.connection_type == ""
                    REMOVE edge._key IN @@mainEdgeCollection
            """
        binds = {
            "@mainEdgeCollection": task.main_edge_collection.name,
            "nodeId": db_item.id,
        }
        task.database.aql.execute(query=query, bind_vars=binds)

        if all((point_a_node, point_b_node)):
            # create geometry type_line
            new_edge = MoEdge(
                _from=point_a_node.id,
                _to=point_b_node.id,
                connection_type=ConnectionType.GEOMETRY_LINE,
                is_trace=False,
                virtual=False,
                source_id=db_item.id,
            )
            new_edges.append(new_edge)

    results = []
    if new_edges:
        for db_edge_raw in task.main_edge_collection.insert_many(
            [i.model_dump(mode="json", by_alias=True) for i in new_edges],
            return_new=True,
        ):
            if isinstance(db_edge_raw, DocumentInsertError):
                raise ValidationError("Edge insert error " + str(db_edge_raw))
            result = DbMoEdge.model_validate(db_edge_raw["new"])
            results.append(result)

    return results, False


def refresh_path_collection(db_items: list[DbMoNode], task: TaskAbstract):
    node_ids = [i.id for i in db_items if not i.grouped_by_tprm]
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
        "@mainCollection": task.main_collection.name,
        "nodeIds": node_ids,
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

    old_path_edges = path_edge_set.difference(main_edge_set)
    if old_path_edges:
        query = """
            FOR old_edge IN @edges
                FOR edge IN @@pathEdgeCollection
                    FILTER edge._from == old_edge._from AND edge._to == old_edge._to
                    REMOVE edge._key IN @@pathEdgeCollection
        """
        binds = {
            "edges": [
                i.model_dump(by_alias=True, mode="json") for i in old_path_edges
            ],
            "@pathEdgeCollection": task.main_path_collection.name,
        }
        task.database.aql.execute(query=query, bind_vars=binds)


def mo_update(task: TaskAbstract, items: list[MO]) -> OperationResponse:
    result = OperationResponse()
    main_tmo_id = task.document.tmo_id
    line_tmo_ids = get_line_tmo_ids(task=task)

    db_items_dict = {
        i.data.id: i for i in find_mo_nodes(items=items, task=task)
    }
    links_to_spread = []
    for item in items:
        # active
        if not item.active:
            result.delete.append(item)
            continue
        elif item.id not in db_items_dict:
            result.create.append(item)
            continue

        # p_id
        db_item = db_items_dict[item.id]
        if db_item.data.p_id != item.p_id:
            edges_to_spread, is_break = parent_changed(
                db_item=db_item,
                item=item,
                result=result,
                task=task,
                main_tmo_id=main_tmo_id,
            )
            if edges_to_spread:
                links_to_spread.extend(edges_to_spread)
            if is_break:
                continue

        # point and geometry_type=line
        if (
            item.point_a_id != db_item.data.point_a_id
            or item.point_b_id != db_item.data.point_b_id
        ):
            created_links, is_break = point_link_changed(
                db_item=db_item, item=item, task=task, line_tmo_ids=line_tmo_ids
            )
            if created_links:
                links_to_spread.extend(created_links)
            if is_break:
                continue

    refresh_path_collection(db_items=list(db_items_dict.values()), task=task)
    spread_connections(task=task, edges=links_to_spread)
    connect_service_by_lines_update(task=task, edges=links_to_spread)
    return result
