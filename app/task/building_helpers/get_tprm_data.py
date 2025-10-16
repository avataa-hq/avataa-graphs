from task.models.incoming_data import TPRM
from task.task_abstract import TaskAbstract


def get_tprm_data(task: TaskAbstract) -> dict[int, TPRM]:
    if not task.group_by_tprm_ids:
        return {}
    tprm_data_query = """
        FOR doc IN @@tmoCollection
            FOR param IN doc.params
                FILTER param.id IN @tprmIds
                RETURN param
    """
    binds = {
        "@tmoCollection": task.config.tmo_collection_name,
        "tprmIds": task.group_by_tprm_ids,
    }
    response = {
        i["id"]: TPRM.model_validate(i)
        for i in task.database.aql.execute(
            query=tprm_data_query, bind_vars=binds
        )
    }
    # sorting
    result = {i: response[i] for i in task.group_by_tprm_ids if i in response}
    return result
