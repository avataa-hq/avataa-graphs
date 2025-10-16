import json

from task.models.dto import DbMoNode, MoNode, PrmDto
from task.task_abstract import TaskAbstract
from updater.updater_parts.helpers.delete_grouping_node import (
    delete_grouping_node,
)
from updater.updater_parts.helpers.group_node import GroupNode


def find_or_create_group_node(
    prm: PrmDto, node: DbMoNode, task: TaskAbstract, parent_mo_id: int | None
) -> GroupNode:
    query = """
        FOR node IN @@mainCollection
            FILTER node.grouped_by_tprm == @tprmId
            FILTER node.group_p_id == @pId
            FILTER node.name == @groupName
            RETURN node
    """
    binds = {
        "@mainCollection": task.main_collection.name,
        "tprmId": prm.tprm_id,
        "pId": parent_mo_id,
    }
    is_new = False
    if not node.data or not node.data.params:
        return GroupNode(node=None, is_new=is_new)
    old_param: PrmDto | None = None
    for db_param in node.data.params:
        if db_param.tprm_id == prm.tprm_id:
            old_param = db_param
            break
    if old_param and old_param != prm:
        name = (
            old_param.parsed_value.value
            if old_param.parsed_value
            else old_param.value
        )
        if not isinstance(name, str):
            name = json.dumps(name, default=str)
        binds["groupName"] = name
        response = [
            DbMoNode.model_validate(i)
            for i in task.database.aql.execute(query=query, bind_vars=binds)
        ]

        if len(response) > 0:
            # found
            old_db_mo_node = DbMoNode.model_validate(response[0])
            delete_grouping_node(
                group_node=old_db_mo_node, task=task, node=node
            )
    name = prm.parsed_value.value if prm.parsed_value else prm.value
    if not isinstance(name, str):
        name = json.dumps(name, default=str)
    binds["groupName"] = name

    response = [
        DbMoNode.model_validate(i)
        for i in task.database.aql.execute(query=query, bind_vars=binds)
    ]

    if len(response) > 0:
        # found
        result = DbMoNode.model_validate(response[0])
        if node.data.id not in set(result.mo_ids):
            result.mo_ids.append(node.data.id)
            response = task.main_collection.update(
                result.model_dump(mode="json", by_alias=True), return_new=True
            )
            result = DbMoNode.model_validate(response["new"])
    else:
        # not found
        new_group_node = MoNode(
            grouped_by_tprm=prm.tprm_id,
            name=name,
            tmo=node.tmo,
            mo_ids=[node.data.id],
            is_trace=False,
            data=None,
            group_p_id=parent_mo_id,
        )
        response = task.main_collection.insert(
            new_group_node.model_dump(mode="json", by_alias=True),
            return_new=True,
        )
        result = DbMoNode.model_validate(response["new"])
        is_new = True
    return GroupNode(node=result, is_new=is_new)
