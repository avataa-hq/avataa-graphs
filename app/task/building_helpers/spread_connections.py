from itertools import groupby

from arango import DocumentInsertError

from task.building_helpers.get_hierarchical_nodes import get_hierarchical_nodes
from task.building_helpers.get_real_links import get_real_links
from task.models.building import HierarchicalDbMo
from task.models.dto import DbMoEdge, MoEdge
from task.models.errors import GraphBuildingError
from task.task_abstract import TaskAbstract


def create_new_connections(
    _from_node: HierarchicalDbMo,
    _to_node: HierarchicalDbMo,
    _real_edge: DbMoEdge,
    _is_tracking: bool,
    _nearest_id: int | None,
    _skip_first_to_node: bool,
):
    _edges = []

    if _is_tracking:
        _edge = MoEdge(
            _from=_from_node.id,
            _to=_to_node.id,
            connection_type=_real_edge.connection_type,
            prm=_real_edge.prm,
            tprm=_real_edge.tprm,
            is_trace=_real_edge.is_trace,
            virtual=True,
            source_id=_real_edge.source_id,
        )
        _edges.append(_edge)
    else:
        if _skip_first_to_node:
            _to_node_current = _to_node.parent
        else:
            _to_node_current = _to_node
        while True:
            if not _to_node_current:
                break
            if _nearest_id == _to_node_current.id:
                break

            _edge = MoEdge(
                _from=_from_node.id,
                _to=_to_node_current.id,
                connection_type=_real_edge.connection_type,
                prm=_real_edge.prm,
                tprm=_real_edge.tprm,
                is_trace=_real_edge.is_trace,
                virtual=True,
                source_id=_real_edge.source_id,
            )
            _edges.append(_edge)

            _to_node_current = _to_node_current.parent
    return _edges


def update_or_create_edges(
    task: TaskAbstract, _virtual_edges: list[MoEdge], _real_edge: DbMoEdge
):
    if not _virtual_edges:
        return
    _sorted_virtual_edges = sorted(_virtual_edges, key=lambda x: x.from_)
    query = """
        FOR doc IN @@mainEdgeCollection
            FILTER doc.virtual == true
            FILTER doc._from == @fromId
            FILTER doc.tprm == @tprm
            FILTER doc.connection_type == @connectionType
            FILTER doc._to IN @toIds
            RETURN doc
    """
    binds = {
        "@mainEdgeCollection": task.config.graph_data_edge_name,
        "tprm": _real_edge.tprm,
        "connectionType": _real_edge.connection_type.value,
    }
    # group by FROM
    for _from_id, edges in groupby(
        _sorted_virtual_edges, key=lambda x: x.from_
    ):
        to_ids = {i.to_: i for i in edges}
        binds["fromId"] = _from_id
        binds["toIds"] = list(to_ids.keys())
        # update if exist edge
        to_update = {}
        for db_edge in task.database.aql.execute(query=query, bind_vars=binds):
            db_edge = DbMoEdge.model_validate(db_edge)
            new_edge = to_ids[db_edge.to_]
            if new_edge.prm:
                if db_edge.prm:
                    db_edge.prm.extend(new_edge.prm)
                else:
                    db_edge.prm = new_edge.prm
            to_update[db_edge.to_] = db_edge
        if to_update:
            items = [
                i.model_dump(mode="json", by_alias=True)
                for i in to_update.values()
            ]
            for response in task.main_edge_collection.update_many(
                items, keep_none=True
            ):
                if isinstance(response, DocumentInsertError):
                    raise GraphBuildingError(
                        f"Virtual edge updating error. {str(response)}"
                    )
        # insert  if not exist
        to_insert = {k: v for k, v in to_ids.items() if k not in to_update}
        if to_insert:
            items = [
                i.model_dump(mode="json", by_alias=True)
                for i in to_insert.values()
            ]
            for response in task.main_edge_collection.insert_many(
                items, keep_none=True
            ):
                if isinstance(response, DocumentInsertError):
                    raise GraphBuildingError(
                        f"Virtual edge creation error. {str(response)}"
                    )


def spread_connection(
    edge: DbMoEdge,
    task: TaskAbstract,
    cached_from_node: HierarchicalDbMo | None = None,
) -> HierarchicalDbMo:
    if cached_from_node is None or cached_from_node.id != edge.from_:
        from_node = get_hierarchical_nodes(node_id=edge.from_, task=task)
        cached_from_node = from_node
    else:
        from_node = cached_from_node

    to_node = get_hierarchical_nodes(node_id=edge.to_, task=task)
    is_tracking = to_node.tmo == task.trace_tmo_id if to_node else False
    if not to_node or (not to_node.parent and not is_tracking):
        return cached_from_node
    nearest_id = from_node.get_nearest_parent_id(to_node)
    if is_tracking:
        from_node_current = from_node.parent
    else:
        from_node_current = from_node
    skip_first_to_node = True
    while True:
        if not from_node_current:
            break
        if nearest_id == from_node_current.id:
            # convergence point
            break
        virtual_edges = create_new_connections(
            _from_node=from_node_current,
            _to_node=to_node,
            _real_edge=edge,
            _is_tracking=is_tracking,
            _nearest_id=nearest_id,
            _skip_first_to_node=skip_first_to_node,
        )
        update_or_create_edges(
            _virtual_edges=virtual_edges, _real_edge=edge, task=task
        )

        # loop
        from_node_current = from_node_current.parent
        skip_first_to_node = False
    return cached_from_node


def spread_connections(task: TaskAbstract, edges: list[DbMoEdge] | None = None):
    cached_from_node: HierarchicalDbMo | None = None
    if edges is None:
        edges = get_real_links(task=task)
    for edge in edges:
        cached_from_node = spread_connection(
            edge=edge, task=task, cached_from_node=cached_from_node
        )
