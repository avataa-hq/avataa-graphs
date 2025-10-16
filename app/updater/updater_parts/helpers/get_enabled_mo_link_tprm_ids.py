from collections import defaultdict

from task.models.dto import DbTmoNode
from task.task_abstract import TaskAbstract


def get_enabled_mo_link_tprm_ids(
    task: TaskAbstract, tmos_dict: dict[int, DbTmoNode]
) -> dict[int, set[int]]:
    results = defaultdict(set)
    if not tmos_dict:
        return results
    query = """
        FOR edge IN @@tmoEdgeCollection
            FILTER edge.enabled == true
            FILTER edge.link_type == "mo_link"
            FILTER edge._from IN @tmoIds
            RETURN {"tmoNodeId": edge._from, "tprmId": edge.tprm_id}
    """
    binds = {
        "@tmoEdgeCollection": task.tmo_edge_collection.name,
        "tmoIds": [
            task.config.get_tmo_collection_key(i.tmo_id)
            for i in tmos_dict.values()
        ],
    }
    response = task.database.aql.execute(query=query, bind_vars=binds)

    for item_raw in response:
        tmo_id = int(item_raw["tmoNodeId"].split("/")[-1])
        results[tmo_id].add(item_raw["tprmId"])
    return results
