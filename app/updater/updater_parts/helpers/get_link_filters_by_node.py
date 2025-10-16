from task.models.dto import DbMoEdge, DbMoNode
from task.task_abstract import TaskAbstract


def get_link_filters_by_node(task: TaskAbstract, node: DbMoNode):
    links_query = """
               FOR edge IN @@mainEdgeCollection
                   FILTER edge._from == @nodeId OR edge._to == @nodeId
                   FILTER edge.connection_type != "p_id"
                   RETURN edge
           """
    binds = {
        "@mainEdgeCollection": task.main_edge_collection.name,
        "nodeId": node.id,
    }
    response = task.database.aql.execute(query=links_query, bind_vars=binds)

    prm_ids: set[int] = set()
    node_ids: set[str] = set()
    for item in response:
        item = DbMoEdge.model_validate(item)
        if item.prm:
            prm_ids.update(item.prm)
        if item.source_id:
            node_ids.add(item.source_id)
    return prm_ids, node_ids
