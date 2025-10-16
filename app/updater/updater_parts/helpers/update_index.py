import json

from task.models.dto import DbMoNode
from task.models.incoming_data import TPRM


def update_index(db_mo_node: DbMoNode, tprms_dict: dict[int, TPRM]):
    index = []
    if db_mo_node.data and db_mo_node.data.params:
        for param in db_mo_node.data.params:
            tprm = tprms_dict.get(param.tprm_id, None)
            if not tprm:
                continue
            if not tprm.returnable:
                continue
            index_value = (
                param.parsed_value.value if param.parsed_value else param.value
            )
            if not isinstance(index_value, str):
                index_value = json.dumps(index_value, default=str)
            index.append(index_value)
    db_mo_node.indexed = index
