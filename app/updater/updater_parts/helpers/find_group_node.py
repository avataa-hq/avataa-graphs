from task.models.dto import DbMoNode
from task.task_abstract import TaskAbstract


def find_group_node(
    tprm_id: int,
    real_mo_node: DbMoNode,
    task: TaskAbstract,
) -> DbMoNode | None:
    prm = None
    for param in real_mo_node.data.params:
        if param.tprm_id == tprm_id:
            prm = param
            break
    if not prm:
        return
    name = prm.parsed_value.value if prm.parsed_value else prm.value
    query = """
        FOR node IN @@mainCollection
            FILTER node.grouped_by_tprm == @tprmId
            FILTER node.name == @name
            FILTER @moId IN node.mo_ids
            LIMIT 1
            RETURN node
    """
    binds = {
        "@mainCollection": task.main_collection.name,
        "name": name,
        "moId": real_mo_node.data.id,
        "tprmId": tprm_id,
    }
    response = list(task.database.aql.execute(query=query, bind_vars=binds))
    if not response:
        return
    result = DbMoNode.model_validate(response[0])
    return result
