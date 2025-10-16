from task.building_helpers.create_links_by_constraint import (
    create_links_by_constraint,
)
from task.building_helpers.find_child_tmos import find_child_tmos
from task.building_helpers.get_constraint_filters_for_edges_by_tmo import (
    get_constraint_filters_for_edges_by_tmo,
)
from task.models.building import ConstraintFilter
from task.models.dto import DbTmoNode
from task.task_abstract import TaskAbstract


def build_links_from_tmo(
    task: TaskAbstract,
    tmo: DbTmoNode,
    recursive: bool = True,
):
    grouped_edges: list[ConstraintFilter] = (
        get_constraint_filters_for_edges_by_tmo(tmo=tmo, task=task)
    )
    for constraint_filter in grouped_edges:
        create_links_by_constraint(
            tmo=tmo, constraint_filter=constraint_filter, task=task
        )

    if recursive:
        # Recursive create children levels links
        for child in find_child_tmos(tmo=tmo, task=task):  # Type: DbTmoEdge
            trace_tmo_id = task.trace_tmo_id
            if child.node.tmo_id == trace_tmo_id:
                continue
            build_links_from_tmo(tmo=child.node, task=task, recursive=recursive)
