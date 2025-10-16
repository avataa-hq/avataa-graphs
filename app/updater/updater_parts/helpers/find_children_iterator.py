from typing import Iterator

from task.models.dto import DbMoNode
from task.task_abstract import TaskAbstract


def find_children_iterator(
    task: TaskAbstract, node: DbMoNode
) -> Iterator[DbMoNode]:
    limit: int = 50

    query = """
        FOR v, e IN 1 INBOUND @nodeId
        GRAPH @mainGraph
            FILTER e.connection_type == "p_id"
            LIMIT @offset, @limit
            RETURN v
    """
    binds = {
        "nodeId": node.id,
        "mainGraph": task.config.graph_data_graph_name,
        "limit": limit,
    }

    offset = 0
    last_response_size = limit
    while limit <= last_response_size:
        binds["offset"] = offset
        chunk = list(task.database.aql.execute(query=query, bind_vars=binds))
        last_response_size = len(chunk)
        offset += last_response_size
        for item in chunk:
            yield DbMoNode.model_validate(item)
