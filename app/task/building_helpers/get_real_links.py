from typing import Iterator

from task.building_helpers.build_from_tmo import QUERY_ITEMS_LIMIT
from task.models.dto import DbMoEdge
from task.task_abstract import TaskAbstract


def get_real_links(task: TaskAbstract) -> Iterator[DbMoEdge]:
    limit = QUERY_ITEMS_LIMIT
    query = """
        FOR doc IN @@mainEdgeCollection
            FILTER (doc.virtual == false) OR (doc.connection_type == "geometry_line")
            FILTER doc.connection_type != "p_id"
            SORT doc._from, doc._to
            LIMIT @offset, @chunkSize
            RETURN doc
            """
    binds = {
        "@mainEdgeCollection": task.config.graph_data_edge_name,
        "chunkSize": limit,
    }
    offset = 0
    last_len = limit
    while last_len >= limit:
        binds["offset"] = offset
        cursor = task.database.aql.execute(query=query, bind_vars=binds)
        chunk = list(cursor)
        last_len = len(chunk)
        offset += last_len
        for doc in chunk:
            edge = DbMoEdge.model_validate(doc)
            yield edge
