import json
from typing import Iterator

from arango import DocumentInsertError

from services.inventory import InventoryInterface
from task.building_helpers.add_indexed_field_to_nodes import (
    add_indexed_filed_to_nodes,
)
from task.building_helpers.fill_prm_values import fill_prm_values
from task.building_helpers.find_child_tmos import find_child_tmos
from task.models.dto import (
    DbMoEdge,
    DbMoNode,
    DbTmoEdge,
    DbTmoNode,
    MoDto,
    MoEdge,
    MoNode,
)
from task.models.enums import ConnectionType
from task.models.errors import GraphBuildingError, NotFound
from task.task_abstract import TaskAbstract

QUERY_ITEMS_LIMIT: int = 1000


def get_mo_nodes_chunk(
    inventory: InventoryInterface, tmo: DbTmoNode, is_trace: bool
) -> Iterator[list[MoNode]]:
    for chunk in inventory.get_mos_by_tmo_id(
        tmo_id=tmo.tmo_id,
        mo_filter_by={"active": True},
        keep_mo_without_prm=True,
    ):
        nodes = []
        for item in chunk:
            mo = MoDto.model_validate(item)
            node = MoNode(
                name=mo.name,
                label=mo.label,
                tmo=mo.tmo_id,
                mo_ids=[mo.id],
                data=mo,
                is_trace=is_trace,
            )
            nodes.append(node)
        fill_prm_values(nodes=nodes, tmo=tmo, inventory=inventory)
        add_indexed_filed_to_nodes(nodes=nodes, tmo=tmo, inventory=inventory)
        yield nodes


def save_mo_nodes_chunk(
    task: TaskAbstract, mo_nodes: list[MoNode]
) -> list[DbMoNode]:
    db_mo_nodes: list[DbMoNode] = []
    if mo_nodes:
        nodes = [i.model_dump(by_alias=True, mode="json") for i in mo_nodes]
        for node in task.main_collection.insert_many(
            nodes, return_new=True, keep_none=True
        ):
            if isinstance(node, DocumentInsertError):
                raise GraphBuildingError(f"Node insertion error. {str(node)}")
            db_node = DbMoNode.model_validate(node["new"])
            db_mo_nodes.append(db_node)
    return db_mo_nodes


def create_edges(
    db_mo_nodes: list[DbMoNode],
    is_trace: bool,
    prev_db_key_by_mo_id: dict[int, str],  # {mo_id: doc._key}
) -> list[MoEdge]:
    edges = []
    if not prev_db_key_by_mo_id:
        return edges
    for node in db_mo_nodes:
        if node.data.p_id in prev_db_key_by_mo_id:
            to_ = prev_db_key_by_mo_id[node.data.p_id]
            edge = MoEdge(
                _from=node.id,
                _to=to_,
                connection_type=ConnectionType.P_ID,
                virtual=False,
                is_trace=is_trace,
            )
            edges.append(edge)
    return edges


def save_edges(task: TaskAbstract, edges: list[MoEdge]) -> list[DbMoEdge]:
    edges = [json.loads(i.model_dump_json(by_alias=True)) for i in edges]
    db_edges = []
    if edges:
        for edge in task.main_edge_collection.insert_many(
            edges, return_new=True, keep_none=True
        ):
            if isinstance(edge, DocumentInsertError):
                raise GraphBuildingError(f"Edge insertion error. {str(edge)}")
            db_edge = DbMoEdge.model_validate(edge["new"])
            db_edges.append(db_edge)
    return db_edges


def get_parent_tmo_node(tmo: DbTmoNode, task: TaskAbstract) -> DbTmoNode | None:
    parent_tmo_query = """
            FOR edge IN @@tmoEdgeCollection
                FILTER edge._from == @childId
                FILTER edge.link_type == "p_id"
                LIMIT 1
                RETURN edge
        """
    binds = {
        "@tmoEdgeCollection": task.tmo_edge_collection.name,
        "childId": tmo.id,
    }
    response = list(
        task.database.aql.execute(query=parent_tmo_query, bind_vars=binds)
    )
    if response:
        parent_response = task.tmo_collection.get(response[0]["_to"])
        if not parent_response:
            raise NotFound(
                f"Parent Node by link id {response[0]['_id']} not found"
            )
        parent_tmo = DbTmoNode.model_validate(parent_response)
        return parent_tmo


def get_prev_db_key_by_mo_id(
    tmo: DbTmoNode, task: TaskAbstract
) -> dict[int, str]:
    prev_db_key_by_mo_id = {}
    parent_tmo_node = get_parent_tmo_node(tmo=tmo, task=task)
    if parent_tmo_node:
        db_mo_nodes_query = """
            FOR doc IN @@mainCollection
                FILTER doc.tmo == @parentId
                FILTER NOT_NULL(doc.data)
                LIMIT @offset, @limit
                RETURN {"_id": doc._id, "moId": doc.data.id}
        """
        binds = {
            "parentId": parent_tmo_node.id,
            "limit": QUERY_ITEMS_LIMIT,
            "@mainCollection": task.main_collection.name,
        }
        last_response_size = QUERY_ITEMS_LIMIT
        offset = 0
        while last_response_size >= QUERY_ITEMS_LIMIT:
            binds["offset"] = offset
            response = list(
                task.database.aql.execute(
                    query=db_mo_nodes_query, bind_vars=binds
                )
            )
            last_response_size = len(response)
            offset += last_response_size
            for item in response:
                prev_db_key_by_mo_id[item["moId"]] = item["_id"]
    return prev_db_key_by_mo_id


def build_from_tmo(
    inventory: InventoryInterface,
    task: TaskAbstract,
    tmo_node: DbTmoNode,
    tmo_edge: DbTmoEdge | None = None,
    prev_db_key_by_mo_id: dict[int, str] | None = None,  # {mo_id: doc._key}
    is_trace: bool = False,
    recursive: bool = True,
):
    if prev_db_key_by_mo_id is None:
        prev_db_key_by_mo_id: dict[int, str] = get_prev_db_key_by_mo_id(
            tmo=tmo_node, task=task
        )
    db_key_by_mo_id: dict[int, str] = {}  # {mo_id: doc._key}
    if tmo_node.enabled or is_trace:
        for nodes_chunk in get_mo_nodes_chunk(
            inventory=inventory, tmo=tmo_node, is_trace=is_trace
        ):
            db_nodes_chunk = save_mo_nodes_chunk(
                task=task, mo_nodes=nodes_chunk
            )
            if tmo_edge and tmo_edge.enabled:
                edges_chunk = create_edges(
                    db_mo_nodes=db_nodes_chunk,
                    is_trace=is_trace,
                    prev_db_key_by_mo_id=prev_db_key_by_mo_id,
                )
                save_edges(task=task, edges=edges_chunk)
            for db_node in db_nodes_chunk:
                db_key_by_mo_id[db_node.data.id] = db_node.id
    if recursive:
        # Recursive create children levels
        for child in find_child_tmos(
            tmo=tmo_node,
            task=task,
        ):  # Type: DbTmoEdge
            child_edge = child.edge
            child_node = child.node
            trace_tmo_id = task.trace_tmo_id
            if child_node.tmo_id == trace_tmo_id:
                continue
            build_from_tmo(
                tmo_node=child_node,
                tmo_edge=child_edge,
                prev_db_key_by_mo_id=db_key_by_mo_id,
                is_trace=is_trace,
                inventory=inventory,
                task=task,
                recursive=recursive,
            )
