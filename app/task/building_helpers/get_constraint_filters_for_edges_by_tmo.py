from itertools import groupby

from task.models.building import ConstraintFilter
from task.models.dto import DbTmoEdge, DbTmoNode
from task.task_abstract import TaskAbstract


def get_constraint_filters_for_edges_by_tmo(
    task: TaskAbstract, tmo: DbTmoNode
) -> list[ConstraintFilter]:
    connection_types_query = """
                FOR doc in @@tmoEdgeCollection
                    FILTER doc.enabled == true
                    FILTER doc.link_type != 'p_id'
                    FILTER doc._from == @tmoDbId
                    RETURN doc
            """
    binds = {"@tmoEdgeCollection": task.config.tmo_edge_name, "tmoDbId": tmo.id}
    db_edges = [
        DbTmoEdge.model_validate(i)
        for i in task.database.aql.execute(
            query=connection_types_query, bind_vars=binds
        )
    ]
    sorted_edges = sorted(db_edges, key=lambda x: x.link_type)
    grouped_edges: list[ConstraintFilter] = []
    for link_type, link_edges in groupby(
        sorted_edges, key=lambda edge: edge.link_type
    ):
        link_edges = sorted(
            link_edges, key=lambda edge: edge.tprm_id if edge.tprm_id else 0
        )
        for tprm_id, tprm_edges in groupby(
            link_edges, key=lambda edge: edge.tprm_id
        ):
            to_tmo_id = [int(i.to_.split("/")[1]) for i in tprm_edges]
            constraint_filter = ConstraintFilter(
                link_type=link_type, tprm_id=tprm_id, to_tmo_id=to_tmo_id
            )
            grouped_edges.append(constraint_filter)
    return grouped_edges
