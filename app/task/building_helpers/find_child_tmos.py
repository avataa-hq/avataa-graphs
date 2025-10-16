from task.models.dto import DbTmoNodeEdge, TmoNode
from task.task_abstract import TaskAbstract


def find_child_tmos(tmo: TmoNode, task: TaskAbstract) -> list[DbTmoNodeEdge]:
    query = """
        FOR v, e IN 1..1 INBOUND @tmo GRAPH @tmoGraph
            FILTER e.link_type IN ["p_id"]
            RETURN {'node': v, 'edge': e}
    """
    # FILTER e.link_type IN ["p_id", "mo_link"]
    # FILTER e.link_type IN ["p_id"]
    binds = {
        "tmo": tmo.id,
        "tmoGraph": task.config.tmo_graph_name,
    }
    try:
        children = list(task.database.aql.execute(query=query, bind_vars=binds))
    except Exception as ex:
        if hasattr(ex, "message"):
            msg = f"AQL Query Failed: {ex.message} {type(ex)}"
        else:
            msg = f"AQL Query Failed: {type(ex)}"
        print(msg)
        print(f"Query: {query}")
        print(f"Bind Vars: {binds}")
        if hasattr(ex, "response"):
            print(f"Raw Response: {ex.response.text}")
        raise
    return [DbTmoNodeEdge.model_validate(i) for i in children]
