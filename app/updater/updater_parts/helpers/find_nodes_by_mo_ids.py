from task.models.dto import DbMoNode
from task.task_abstract import TaskAbstract


def find_nodes_by_mo_ids(
    task: TaskAbstract, mo_ids: list[int]
) -> list[DbMoNode]:
    results = []
    if not mo_ids:
        return results
    query = """
        FOR node IN @@mainCollection
            FILTER node.data.id IN @moIds
            RETURN node
    """
    binds = {"@mainCollection": task.main_collection.name, "moIds": mo_ids}
    response = task.database.aql.execute(query=query, bind_vars=binds)
    results = [DbMoNode.model_validate(i) for i in response]
    return results
