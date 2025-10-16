from collections import namedtuple
from itertools import groupby
import json
from typing import Iterator

from services.inventory import InventoryInterface
from task.building_helpers.build_from_tmo import save_edges, save_mo_nodes_chunk
from task.building_helpers.get_tprm_data import get_tprm_data
from task.models.dto import DbMoNode, MoEdge, MoNode
from task.models.enums import ConnectionType
from task.models.incoming_data import MO, PRM, TPRM
from task.task_abstract import TaskAbstract

NodesByTprmResponse = namedtuple(
    "NodesByTprmResponse",
    ["p_id", "param_value", "children_ids", "mo_ids", "p_edge_id"],
)


def drop_p_id_connections(from_: list[str], to_: str, task: TaskAbstract):
    if not to_:
        return
    query = """
        FOR doc IN @@mainEdgeCollection
            FILTER doc.connection_type == "p_id"
            FILTER doc._to != @toId
            FILTER doc._from IN @fromList
            REMOVE doc._key IN @@mainEdgeCollection
    """
    binds = {
        "@mainEdgeCollection": task.config.graph_data_edge_name,
        "toId": to_,
        "fromList": from_,
    }
    task.database.aql.execute(query=query, bind_vars=binds)


def create_group_node(
    group_name,
    tprm: TPRM,
    mo_ids: list[int],
    inventory: InventoryInterface,
    p_id: int,
) -> MoNode:
    if tprm.val_type in (
        ConnectionType.MO_LINK.value,
        ConnectionType.TWO_WAY_MO_LINK.value,
    ):
        if tprm.multiple:
            mos: list[dict] = inventory.get_mos_by_mo_ids(mo_ids=group_name)
            mos: list[MO] = [MO.model_validate(i) for i in mos]
            group_name = [i.name for i in mos]
        else:
            mo: list[dict] = inventory.get_mos_by_mo_ids(mo_ids=[group_name])
            mo: MO = MO.model_validate(mo[0])
            group_name = mo.name
    elif tprm.val_type == "prm_link":
        if tprm.multiple:
            prms: list[dict] = inventory.get_prms_by_prm_ids(prm_ids=group_name)
            prms: list[PRM] = [PRM.model_validate(i) for i in prms]
            group_name = [i.value for i in prms]
        else:
            prms: list[dict] = inventory.get_prms_by_prm_ids(
                prm_ids=[group_name]
            )
            prm: PRM = PRM.model_validate(prms[0])
            group_name = prm.value
    name = (
        group_name
        if isinstance(group_name, str)
        else json.dumps(group_name, default=str)
    )
    new_node = MoNode(
        grouped_by_tprm=tprm.id,
        group_p_id=p_id,
        name=name,
        tmo=tprm.tmo_id,
        mo_ids=mo_ids,
        is_trace=False,
        data=None,
    )
    return new_node


def create_connections(from_: list[str], to_: str, db_node: DbMoNode):
    new_edges: list[MoEdge] = []
    if to_:
        to_parent = MoEdge(
            _from=db_node.id,
            _to=to_,
            connection_type=ConnectionType.P_ID,
            is_trace=False,
            virtual=False,
        )
        new_edges.append(to_parent)
    for child_id in from_:
        edge = MoEdge(
            _from=child_id,
            _to=db_node.id,
            connection_type=ConnectionType.P_ID,
            is_trace=False,
            virtual=False,
        )
        new_edges.append(edge)
    return new_edges


def sort_by_parent(grouped_data: dict) -> str:
    if len(grouped_data["p_edges"]) == 0:
        return ""

    p_edge = grouped_data["p_edges"][0]
    return p_edge.get("_to", "")


def get_nodes_by_tprm(
    task: TaskAbstract, tprm_id: int
) -> Iterator[NodesByTprmResponse]:
    query = """
        FOR doc IN @@mainCollection
        FILTER NOT_NULL(doc.data.params)
        FOR param IN doc.data.params
            FILTER param.tprm_id == @tprmId
            LET edges = (FOR edge IN @@mainEdgeCollection
                    FILTER edge._from == doc._id
                    FILTER edge.connection_type == "p_id"
                    LIMIT 1
                    RETURN edge)

            RETURN {"id": doc._id, "mo_ids": doc.mo_ids, "tmo_id": doc.tmo, "param": param, "p_edges": edges,
                    "p_id": doc.data.p_id}
        """
    binds = {
        "@mainCollection": task.config.graph_data_collection_name,
        "@mainEdgeCollection": task.config.graph_data_edge_name,
        "tprmId": tprm_id,
    }
    response = list(task.database.aql.execute(query=query, bind_vars=binds))
    response = sorted(response, key=sort_by_parent)
    for p_edge_id, p_id_records in groupby(response, sort_by_parent):
        if p_edge_id == "":
            p_edge_id = None
        p_id_records = sorted(p_id_records, key=lambda x: x["param"]["value"])
        p_id = p_id_records[0]["p_id"]
        for param_value, param_records in groupby(
            p_id_records, lambda x: x["param"]["value"]
        ):
            children_ids = []
            mo_ids = set()
            for record in param_records:
                children_ids.append(record["id"])
                mo_ids.update(record["mo_ids"])
            yield NodesByTprmResponse(
                p_id=p_id,
                param_value=param_value,
                children_ids=children_ids,
                mo_ids=mo_ids,
                p_edge_id=p_edge_id,
            )


def group_nodes(task: TaskAbstract, inventory: InventoryInterface):
    if not task.group_by_tprm_ids:
        return
    tprm_data = get_tprm_data(task=task)
    for tprm_id, tprm in tprm_data.items():
        for node_by_tprm in get_nodes_by_tprm(task=task, tprm_id=tprm_id):
            node = create_group_node(
                group_name=node_by_tprm.param_value,
                tprm=tprm,
                mo_ids=list(node_by_tprm.mo_ids),
                p_id=node_by_tprm.p_id,
                inventory=inventory,
            )
            db_node = save_mo_nodes_chunk(task=task, mo_nodes=[node])
            edges = create_connections(
                from_=node_by_tprm.children_ids,
                to_=node_by_tprm.p_edge_id,
                db_node=db_node[0],
            )
            save_edges(edges=edges, task=task)
            drop_p_id_connections(
                from_=node_by_tprm.children_ids,
                to_=node_by_tprm.p_edge_id,
                task=task,
            )
