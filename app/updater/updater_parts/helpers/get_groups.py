from collections import defaultdict

from task.task_abstract import TaskAbstract


def get_groups(task: TaskAbstract) -> defaultdict[int, list[int]]:
    result = defaultdict(list)

    group_by_tprm_ids = task.group_by_tprm_ids
    if group_by_tprm_ids:
        # get
        query = """
            FOR node IN @@tmoCollection
                FILTER NOT_NULL(node.params)
                FOR param in node.params
                    FILTER param.id IN @tprmIds
                    RETURN {"tmoId": node.id, 'tprmId': param.id}
        """
        binds = {
            "@tmoCollection": task.tmo_collection.name,
            "tprmIds": group_by_tprm_ids,
        }
        for raw_data in task.database.aql.execute(query=query, bind_vars=binds):
            result[raw_data["tmoId"]].append(raw_data["tprmId"])

        # order
        group_tprms_order = {
            tprm_id: index for index, tprm_id in enumerate(group_by_tprm_ids)
        }
        for key, values in result.items():
            ordered_values = sorted(values, key=lambda x: group_tprms_order[x])
            result[key] = list(ordered_values)
    return result
