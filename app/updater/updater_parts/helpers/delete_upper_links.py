from task.task_abstract import TaskAbstract


def get_parent_breadcrumbs(breadcrumbs: str) -> list[str]:
    results = ["/"]
    split = [i for i in breadcrumbs.split("/") if i]
    buffer = []
    for part in split:
        buffer.append(part)
        result = f"/{'/'.join(buffer)}/"
        results.append(result)
    return results


def delete_upper_links(
    task: TaskAbstract, prm_ids: set[int], node_ids: set[str], breadcrumbs: str
):
    parent_breadcrumbs = get_parent_breadcrumbs(breadcrumbs=breadcrumbs)
    query = """
        FOR edge IN @@mainEdgeCollection
            FILTER edge.prms IN @prmIds OR edge.source_id IN @moIds
            FILTER edge.virtual == false
            FILTER edge.breadcrumbs IN @breadcrumbs
            REMOVE edge._key IN @@mainEdgeCollection
    """
    binds = {
        "@mainEdgeCollection": task.main_edge_collection.name,
        "prmIds": list(prm_ids),
        "moIds": list(node_ids),
        "breadcrumbs": parent_breadcrumbs,
    }
    task.database.aql.execute(query=query, bind_vars=binds)
