from task.models.dto import DbMoNode
from task.task_abstract import TaskAbstract


def delete_group_link(
    task: TaskAbstract, db_mo_node: DbMoNode, group_node: DbMoNode
):
    query = """
        FOR edge IN @@mainEdgeCollection
            FILTER edge._to == @groupDbId
            FILTER edge.connection_type == "p_id"
            FOR node IN @@mainCollection
                FILTER node._id == edge._from
                FILTER @moId IN node.mo_ids
                LIMIT 1
                REMOVE edge._key IN @@mainEdgeCollection
    """
    binds = {
        "@mainEdgeCollection": task.main_edge_collection.name,
        "groupDbId": group_node.id,
        "@mainCollection": task.main_collection.name,
        "moId": db_mo_node.data.id,
    }
    task.database.aql.execute(query=query, bind_vars=binds)
