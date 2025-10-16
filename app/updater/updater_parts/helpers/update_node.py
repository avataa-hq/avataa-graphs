from services.inventory import InventoryInterface
from task.helpers.convert_prms import update_prm
from task.models.dto import DbMoNode, PrmDto
from task.models.incoming_data import PRM, TPRM
from task.task_abstract import TaskAbstract
from updater.updater_parts.helpers.update_index import update_index


def update_node(
    task: TaskAbstract,
    db_mo_node: DbMoNode,
    item: PRM,
    tprms_dict: dict[int, TPRM],
    inventory: InventoryInterface,
):
    tprm = tprms_dict.get(item.tprm_id, None)
    if not tprm:
        return
    prm = PrmDto.model_validate(item.model_dump(mode="json", by_alias=True))
    prm = update_prm(prm=prm, tprm=tprm, inventory=inventory)
    db_mo_node.data.params = [
        i for i in db_mo_node.data.params if i.id != item.id
    ]
    db_mo_node.data.params.append(prm)
    update_index(db_mo_node=db_mo_node, tprms_dict=tprms_dict)
    response = task.main_collection.update(
        db_mo_node.model_dump(mode="json", by_alias=True), return_new=True
    )
    return DbMoNode.model_validate(response["new"])
