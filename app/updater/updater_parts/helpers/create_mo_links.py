from arango import DocumentInsertError

from task.building_helpers.fill_path_edge_collection import UniqueFromToEdge
from task.models.dto import DbMoEdge, DbMoNode, MoEdge
from task.models.enums import ConnectionType
from task.models.errors import ValidationError
from task.models.incoming_data import PRM
from task.task_abstract import TaskAbstract
from updater.updater_parts.helpers.find_nodes_by_mo_ids import (
    find_nodes_by_mo_ids,
)


def create_mo_links(
    task: TaskAbstract,
    item: PRM,
    db_mo_node: DbMoNode,
    mo_link_tprm_ids: dict[int, set[int]],
    trace_tmo_id: int | None = None,
) -> list[DbMoEdge]:
    results = []
    if not mo_link_tprm_ids:
        return results
    mo_link_tprm_id = mo_link_tprm_ids.get(db_mo_node.tmo, set())
    if item.tprm_id not in mo_link_tprm_id:
        return results
    mo_ids = item.value if isinstance(item.value, list) else [item.value]
    other_side_nodes = find_nodes_by_mo_ids(task=task, mo_ids=mo_ids)
    new_edges = []
    for other_side_node in other_side_nodes:
        new_edge = MoEdge(
            _from=db_mo_node.id,
            _to=other_side_node.id,
            connection_type=ConnectionType.MO_LINK,
            prm=[item.id],
            tprm=item.tprm_id,
            virtual=False,
            source_id=db_mo_node.id,
            is_trace=other_side_node.tmo == trace_tmo_id,
        )
        new_edges.append(new_edge)
    if not new_edges:
        return results
    new_edge_dicts = [
        i.model_dump(mode="json", by_alias=True) for i in new_edges
    ]
    response = task.main_edge_collection.insert_many(
        new_edge_dicts, return_new=True
    )
    for edge_raw in response:
        if isinstance(edge_raw, DocumentInsertError):
            raise ValidationError("Can not create edge " + str(edge_raw))
        result = DbMoEdge.model_validate(edge_raw["new"])
        results.append(result)
    path_edge_dicts = [
        UniqueFromToEdge.model_validate(i).model_dump(
            mode="json", by_alias=True
        )
        for i in new_edge_dicts
    ]
    task.main_path_collection.insert_many(path_edge_dicts)
    return results
