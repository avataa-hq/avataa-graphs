from task.models.dto import DbTmoNode
from task.task_abstract import TaskAbstract


def get_enabled_tmo_nodes_by_tmo_ids(
    task: TaskAbstract, tmo_ids: list[int]
) -> dict[int, DbTmoNode]:
    query = """
        FOR node IN @@tmoCollection
            FILTER node.id IN @tmoIds
            FILTER node.enabled == true
            RETURN node
    """
    binds = {"@tmoCollection": task.tmo_collection.name, "tmoIds": tmo_ids}
    response = task.database.aql.execute(query=query, bind_vars=binds)
    results = {}
    for item_raw in response:
        result = DbTmoNode.model_validate(item_raw)
        results[result.tmo_id] = result
    return results
