from dataclasses import dataclass, field

from services.inventory import InventoryInterface
from task.models.dto import (
    DbTmoNode,
    MoNode,
    ParsedValue,
    ParsedValueTriggers,
    PrmDto,
)
from task.models.enums import ConnectionType
from task.models.incoming_data import PRM, TPRM


@dataclass(slots=True)
class GroupedParams:
    mo_link_params: list = field(default_factory=list)
    two_way_mo_link_params: list = field(default_factory=list)
    prm_link_params: list = field(default_factory=list)


def group_prms_by_type(
    nodes: list[MoNode],
    mo_link_params: dict[int, TPRM],
    two_way_mo_link_params: dict[int, TPRM],
    prm_link_params: dict[int, TPRM],
) -> GroupedParams:
    # collecting stage
    mo_links_ids = set(mo_link_params.keys())
    two_way_mo_links_ids = set(two_way_mo_link_params.keys())
    prm_links_ids = set(prm_link_params.keys())

    mo_link_params_ = []
    prm_link_params_ = []
    two_way_mo_link_params_ = []
    for node in nodes:
        if not node.data or not node.data.params:
            continue
        for param in node.data.params:
            if param.tprm_id in mo_links_ids:
                mo_link_params_.append(param)
            elif param.tprm_id in two_way_mo_links_ids:
                two_way_mo_link_params_.append(param)
            elif param.tprm_id in prm_links_ids:
                prm_link_params_.append(param)
    return GroupedParams(
        mo_link_params=mo_link_params_,
        prm_link_params=prm_link_params_,
        two_way_mo_link_params=two_way_mo_link_params_,
    )


def fill_by_mo_link(
    params: list[PrmDto], inventory: InventoryInterface
) -> list[PrmDto]:
    unique_mo_ids = set()
    for param in params:
        if isinstance(param.value, list):
            unique_mo_ids.update(param.value)
        else:
            unique_mo_ids.add(int(param.value))
    mo_name_dict = {
        i["id"]: i["name"]
        for i in inventory.get_mos_by_mo_ids(mo_ids=list(unique_mo_ids))
    }
    for param in params:
        is_multiple = isinstance(param.value, list)
        parsed_values = []
        tmp_values = param.value if is_multiple else [param.value]
        for tmp_value in tmp_values:
            mo_name = mo_name_dict.get(tmp_value, "")
            parsed_values.append(mo_name)
        parsed_value = ParsedValue(
            raw_value=param.value,
            value=parsed_values if is_multiple else parsed_values[0],
            triggers=ParsedValueTriggers(mos=tmp_values),
        )
        param.parsed_value = parsed_value
    return params


def fill_by_prm_link(
    params: list[PrmDto], inventory: InventoryInterface
) -> list[PrmDto]:
    unique_prm_ids = set()
    for param in params:
        if isinstance(param.value, list):
            unique_prm_ids.update(param.value)
        else:
            unique_prm_ids.add(int(param.value))
    prm_dict = {
        i["id"]: PRM.model_validate(i)
        for i in inventory.get_prms_by_prm_ids(prm_ids=list(unique_prm_ids))
    }
    for param in params:
        is_multiple = isinstance(param.value, list)
        parsed_values = []
        trigger_mos = set()
        tmp_values = param.value if is_multiple else [param.value]
        for tmp_value in tmp_values:
            prm = prm_dict[tmp_value]
            parsed_values.append(prm.value)
            trigger_mos.add(prm.mo_id)
        parsed_value = ParsedValue(
            raw_value=param.value,
            value=parsed_values if is_multiple else parsed_values[0],
            triggers=ParsedValueTriggers(mos=trigger_mos, prms=tmp_values),
        )
        param.parsed_value = parsed_value
    return params


def fill_by_two_way_mo_link(
    params: list[PrmDto], inventory: InventoryInterface
) -> list[PrmDto]:
    return fill_by_prm_link(params=params, inventory=inventory)


def fill_prm_values(
    nodes: list[MoNode], tmo: DbTmoNode, inventory: InventoryInterface
) -> list[MoNode]:
    mo_link_params: dict[int, TPRM] = {}
    two_way_mo_link_params: dict[int, TPRM] = {}
    prm_link_params: dict[int, TPRM] = {}
    for param in tmo.params:
        match param.val_type:
            case ConnectionType.MO_LINK.value:
                mo_link_params[param.id] = param
            case "prm_link":
                prm_link_params[param.id] = param
            case ConnectionType.TWO_WAY_MO_LINK.value:
                two_way_mo_link_params[param.id] = param
    grouped_tprms = group_prms_by_type(
        nodes=nodes,
        mo_link_params=mo_link_params,
        prm_link_params=prm_link_params,
        two_way_mo_link_params=two_way_mo_link_params,
    )
    if grouped_tprms.mo_link_params:
        fill_by_mo_link(
            params=grouped_tprms.mo_link_params, inventory=inventory
        )
    if grouped_tprms.two_way_mo_link_params:
        fill_by_two_way_mo_link(
            params=grouped_tprms.two_way_mo_link_params, inventory=inventory
        )
    if grouped_tprms.prm_link_params:
        fill_by_prm_link(
            params=grouped_tprms.prm_link_params, inventory=inventory
        )
    return nodes
