from task.models.building import HierarchicalDbMo
from task.task_abstract import TaskAbstract


def get_hierarchical_nodes(
    task: TaskAbstract, node_id: str
) -> HierarchicalDbMo | None:
    node = task.main_collection.get(node_id)
    if node is None:
        return
    node = HierarchicalDbMo.model_validate(node)

    query = """
        FOR edge IN @@mainEdgeCollection
            FILTER edge._from == @nodeId
            FILTER edge.connection_type == "p_id"
            LIMIT 1
            RETURN edge._to
    """
    binds = {
        "@mainEdgeCollection": task.config.graph_data_edge_name,
        "nodeId": node_id,
    }
    response = list(task.database.aql.execute(query=query, bind_vars=binds))
    if response:
        parent_node_id = response[0]
        node.parent = get_hierarchical_nodes(node_id=parent_node_id, task=task)
    return node
