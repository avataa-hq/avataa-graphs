from task.models.dto import DbMoNode
from task.task_abstract import TaskAbstract


def find_node_by_mo_id(
    task: TaskAbstract, mo_id: int | None
) -> DbMoNode | None:
    if not mo_id:
        return
    query = """
        FOR node IN @@mainCollection
            FILTER node.data.id == @moId
            RETURN node
    """
    binds = {"@mainCollection": task.main_collection.name, "moId": mo_id}
    response = task.database.aql.execute(query=query, bind_vars=binds)
    results = [DbMoNode.model_validate(i) for i in response]
    result = results[0] if results else None
    return result
