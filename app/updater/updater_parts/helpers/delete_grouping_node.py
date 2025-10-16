from task.models.dto import DbMoNode
from task.task_abstract import TaskAbstract
from updater.updater_parts.helpers.delete_upper_links import delete_upper_links
from updater.updater_parts.helpers.get_link_filters_by_node import (
    get_link_filters_by_node,
)


def delete_grouping_node(
    group_node: DbMoNode, task: TaskAbstract, node: DbMoNode
):
    query = """
            FOR v, e IN OUTBOUND SHORTEST_PATH @childId TO @parentId
            GRAPH @mainGraph
                FILTER e.connection_type == "p_id"
                RETURN v
        """
    binds = {
        "childId": node.id,
        "parentId": group_node.id,
        "mainGraph": task.config.graph_data_graph_name,
    }
    mid_nodes = [
        DbMoNode.model_validate(i)
        for i in task.database.aql.execute(query=query, bind_vars=binds)
    ]

    all_nodes = [*mid_nodes, node]
    for index in range(len(all_nodes) - 1):
        parent_node = all_nodes[index]
        child_node = all_nodes[index + 1]

        parent_node.mo_ids.remove(node.data.id)
        prm_ids, node_ids = get_link_filters_by_node(task=task, node=child_node)
        delete_upper_links(
            task=task,
            prm_ids=prm_ids,
            node_ids=node_ids,
            breadcrumbs=parent_node.breadcrumbs,
        )
        if parent_node.mo_ids:
            task.main_collection.update(
                parent_node.model_dump(by_alias=True, mode="json"),
                check_rev=False,
            )
        else:
            task.main_collection.delete(
                parent_node.model_dump(by_alias=True, mode="json"),
                check_rev=False,
            )
