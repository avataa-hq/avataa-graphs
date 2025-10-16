from collections import defaultdict, namedtuple
import json

from services.inventory import InventoryInterface
from task.models.dto import DbMoNode, MoNode
from task.models.enums import ConnectionType
from task.models.incoming_data import PRM, TMO, TPRM

GroupedParams = namedtuple(
    "GroupedParams",
    [
        "simple_returnable_params",
        "mo_links",
        "two_way_mo_links",
        "prm_links",
        "other_multiple_params",
        "returnable",
    ],
)

SortedParams = namedtuple(
    "SortedParams",
    ["simple_index", "mo_links", "two_way_mo_links", "prm_links"],
)


def stringify_value(value) -> str:
    if isinstance(value, str):
        return value
    return json.dumps(value, default=str)


def group_params(tmo: TMO) -> GroupedParams | None:
    simple_returnable_params: dict[int, TPRM] = {}
    mo_links: dict[int, TPRM] = {}
    two_way_mo_links: dict[int, TPRM] = {}
    prm_links: dict[int, TPRM] = {}
    other_multiple_params: dict[int, TPRM] = {}
    for i in tmo.params:
        if not i.returnable:
            continue
        if i.val_type == ConnectionType.MO_LINK.value:
            mo_links[i.id] = i
        elif i.val_type == ConnectionType.TWO_WAY_MO_LINK.value:
            two_way_mo_links[i.id] = i
        elif i.val_type == "prm_link":
            prm_links[i.id] = i
        elif i.multiple:
            other_multiple_params[i.id] = i
        else:
            simple_returnable_params[i.id] = i
    returnable = {
        *simple_returnable_params,
        *mo_links,
        *two_way_mo_links,
        *prm_links,
        *other_multiple_params,
    }
    return GroupedParams(
        simple_returnable_params=simple_returnable_params,
        mo_links=mo_links,
        two_way_mo_links=two_way_mo_links,
        prm_links=prm_links,
        other_multiple_params=other_multiple_params,
        returnable=returnable,
    )


def sort_params(
    params: list[PRM], grouped_params: GroupedParams
) -> SortedParams:
    mo_links = []
    two_way_mo_links = []
    prm_links = []
    indexed_fields = []
    for param in params:
        if param.tprm_id not in grouped_params.returnable:
            continue
        if param.tprm_id in grouped_params.mo_links:
            if isinstance(param.value, list):
                mo_links.extend(param.value)
            else:
                mo_links.append(param.value)
        elif param.tprm_id in grouped_params.two_way_mo_links:
            two_way_mo_links.append(param.value)
        elif param.tprm_id in grouped_params.prm_links:
            if isinstance(param.value, list):
                prm_links.extend(param.value)
            else:
                prm_links.append(param.value)
        else:
            if isinstance(param.value, list):
                indexed_fields.extend(param.value)
            else:
                indexed_fields.append(param.value)
    stringified_params = [stringify_value(field) for field in indexed_fields]
    return SortedParams(
        simple_index=stringified_params,
        mo_links=mo_links,
        prm_links=prm_links,
        two_way_mo_links=two_way_mo_links,
    )


def add_to_index_prm_mo_links_data(
    mo_links: dict[int, list[MoNode]],
    two_way_mo_links: dict[int, list[MoNode]],
    prm_links: dict[int, list[MoNode]],
    inventory: InventoryInterface,
):
    if mo_links:
        for mo in inventory.get_mos_by_mo_ids(
            mo_ids=[int(key) for key in mo_links.keys()]
        ):
            for node in mo_links[mo["id"]]:
                node.indexed.append(mo["name"])
                if label := mo.get("label", ""):
                    node.indexed.append(label)
    if two_way_mo_links:
        for mo in inventory.get_mos_by_mo_ids(
            mo_ids=[int(key) for key in two_way_mo_links.keys()]
        ):
            for node in two_way_mo_links[mo["id"]]:
                node.indexed.append(mo["name"])
                if label := mo.get("label", ""):
                    node.indexed.append(label)
    if prm_links:
        for prm in inventory.get_prms_by_prm_ids(
            prm_ids=[int(key) for key in prm_links.keys()]
        ):
            for node in prm_links[prm["id"]]:
                if isinstance(prm["value"], list):
                    node.indexed.extend(
                        [stringify_value(i) for i in prm["value"]]
                    )
                else:
                    node.indexed.append(stringify_value(prm["value"]))


def add_indexed_filed_to_nodes(
    nodes: list[MoNode | DbMoNode], tmo: TMO, inventory: InventoryInterface
):
    grouped_params = group_params(tmo=tmo)
    if not grouped_params.returnable:
        return

    prm_mo_links: dict[int, list[MoNode]] = defaultdict(list)
    two_way_mo_links: dict[int, list[MoNode]] = defaultdict(list)
    prm_prm_links: dict[int, list[MoNode]] = defaultdict(list)
    for node in nodes:
        sorted_params = sort_params(
            params=node.data.params, grouped_params=grouped_params
        )
        node.indexed = sorted_params.simple_index
        for value in sorted_params.mo_links:
            prm_mo_links[value].append(node)
        for value in sorted_params.two_way_mo_links:
            two_way_mo_links[value].append(node)
        for value in sorted_params.prm_links:
            prm_prm_links[value].append(node)
    add_to_index_prm_mo_links_data(
        mo_links=prm_mo_links,
        two_way_mo_links=two_way_mo_links,
        prm_links=prm_prm_links,
        inventory=inventory,
    )
    return nodes
