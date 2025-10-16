from task.models.incoming_data import PRM, TPRM
from task.task_abstract import TaskAbstract


def get_tprms_dict(task: TaskAbstract, items: list[PRM]) -> dict[int, TPRM]:
    result: dict[int, TPRM] = {}
    if not items:
        return result
    unique_tprm_ids = set(i.tprm_id for i in items)
    query = """
        FOR node IN @@tmoCollection
            FILTER NOT_NULL(node.params)
            FOR tprm in node.params
                FILTER tprm.id IN @tprmIds
                RETURN tprm
    """
    binds = {
        "@tmoCollection": task.tmo_collection.name,
        "tprmIds": list(unique_tprm_ids),
    }
    response = task.database.aql.execute(query=query, bind_vars=binds)
    for tprm_raw in response:
        tprm = TPRM.model_validate(tprm_raw)
        result[tprm.id] = tprm
    return result
