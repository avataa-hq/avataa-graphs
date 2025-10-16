from task.task_abstract import TaskAbstract


def get_line_tmo_ids(task: TaskAbstract) -> set[int]:
    query = """
        FOR node IN @@tmoCollection
            FILTER node.geometry_type == "line"
            RETURN node.id
    """
    binds = {"@tmoCollection": task.tmo_collection.name}
    results = set(task.database.aql.execute(query=query, bind_vars=binds))
    return results
