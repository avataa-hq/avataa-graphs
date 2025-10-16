import pickle

from services.inventory import InventoryInterface
from task.models.dto import ParsedValue, ParsedValueTriggers, PrmDto
from task.models.incoming_data import MO, PRM, TPRM
from task.task_abstract import TaskAbstract


def get_tprms_dict(tprm_ids: list[int], task: TaskAbstract) -> dict[int, TPRM]:
    tprms_dict = {}
    if not tprm_ids:
        return tprms_dict
    query = """
        FOR node IN @@tmoCollection
            FILTER NOT_NULL(node.params)
            FOR param IN node.params
                FILTER param.id IN @tprmIds
                RETURN param
    """
    binds = {
        "@tmoCollection": task.tmo_collection.name,
        "tprmIds": tprm_ids,
    }
    for tprm in task.database.aql.execute(query=query, bind_vars=binds):
        tprm = TPRM.model_validate(tprm)
        tprms_dict[tprm.id] = tprm
    return tprms_dict


def get_mo_link_data(
    value: int | list[int], inventory: InventoryInterface, is_multiple: bool
) -> ParsedValue:
    if isinstance(value, str):
        value = int(value)
    tmp_value = value if is_multiple else [value]
    mos = [
        MO.model_validate(i)
        for i in inventory.get_mos_by_mo_ids(mo_ids=tmp_value)
    ]
    parsed_value = [i.name for i in mos]
    if not is_multiple:
        parsed_value = parsed_value[0]
    triggers = ParsedValueTriggers(mos=tmp_value)
    result = ParsedValue(raw_value=value, value=parsed_value, triggers=triggers)
    return result


def convert_prm_link_value(value, tprm: TPRM, converter: dict):
    result = value
    if tprm.multiple:
        result = pickle.loads(bytes.fromhex(value))
    elif tprm.val_type in converter:
        result = converter[tprm.val_type](value)
    return result


def get_prm_link_data(
    value: int | list[int], inventory: InventoryInterface, is_multiple: bool
) -> ParsedValue:
    tmp_value = value if is_multiple else [value]
    prms = []
    tprm_ids = set()
    for item in inventory.get_prms_by_prm_ids(prm_ids=tmp_value):
        prm = PRM.model_validate(item)
        prms.append(prm)
        tprm_ids.add(prm.tprm_id)
    tprms_dict = {
        i["id"]: TPRM.model_validate(i)
        for i in inventory.get_tprms_by_tprm_id(tprm_ids=list(tprm_ids))
    }
    parsed_value = [
        convert_prm_link_value(
            value=prm.value,
            tprm=tprms_dict[prm.tprm_id],
            converter=inventory.CONVERTER,
        )
        for prm in prms
    ]
    if not is_multiple:
        parsed_value = parsed_value[0]
    triggers = ParsedValueTriggers(prms=tmp_value)
    result = ParsedValue(raw_value=value, value=parsed_value, triggers=triggers)
    return result


def update_prm(prm: PrmDto, tprm: TPRM | None, inventory: InventoryInterface):
    if not tprm:
        tprm = inventory.get_tprms_by_tprm_id(tprm_ids=[prm.tprm_id])
        if not tprm:
            return prm
        tprm = TPRM.model_validate(tprm[0])
    if tprm.multiple and not isinstance(prm.value, list):
        prm.value = pickle.loads(bytes.fromhex(prm.value))
    match tprm.val_type:
        case "mo_link":
            prm.parsed_value = get_mo_link_data(
                value=prm.value, inventory=inventory, is_multiple=tprm.multiple
            )
        case "prm_link":
            prm.parsed_value = get_prm_link_data(
                value=prm.value, inventory=inventory, is_multiple=tprm.multiple
            )
    return prm


def convert_prms(
    prms: list[PRM], inventory: InventoryInterface
) -> list[PrmDto]:
    results = []
    if not prms:
        return results
    unique_tprm_ids = list(set(i.tprm_id for i in prms))
    tprms_dict = {
        i["id"]: TPRM.model_validate(i)
        for i in inventory.get_tprms_by_tprm_id(tprm_ids=list(unique_tprm_ids))
    }
    for prm in prms:
        prm = PrmDto.model_validate(prm.model_dump(by_alias=True))
        if prm.tprm_id in tprms_dict:
            tprm = tprms_dict.get(prm.tprm_id, None)
            prm = update_prm(prm=prm, tprm=tprm, inventory=inventory)
        results.append(prm)
    return results
