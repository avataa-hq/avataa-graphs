from arango.database import StandardDatabase, TransactionDatabase

from task.models.dto import DbMoEdge, DbMoNode
from task.models.enums import ConnectionType


def convert_geometry_line(
    edges: list[DbMoEdge],
    main_collection: str,
    main_edge_collection: str,
    database: TransactionDatabase | StandardDatabase,
) -> tuple[list[DbMoNode], list[DbMoEdge]]:
    line_edges = []
    new_edges = []
    query = """
        FOR doc IN @@mainCollection
            FILTER doc._id IN @nodeIds
            LET edges = (
                FOR edge IN @@mainEdgeCollection
                    FILTER edge._from == doc._id
                    FILTER edge._to IN @toNodeIds
                    FILTER edge.connection_type IN ["point_a", "point_b"]
                    RETURN edge
            )
            RETURN {"node": doc, "edges": edges}
    """
    for edge in edges:
        if edge.connection_type == ConnectionType.GEOMETRY_LINE.value:
            line_edges.append(edge)
        else:
            new_edges.append(edge)
    if not line_edges:
        return [], new_edges

    node_ids = []
    to_node_ids = []
    for edge in line_edges:
        node_ids.append(edge.source_id)
        to_node_ids.extend([edge.from_, edge.to_])
    binds = {
        "@mainCollection": main_collection,
        "@mainEdgeCollection": main_edge_collection,
        "nodeIds": node_ids,
        "toNodeIds": to_node_ids,
    }
    new_nodes = []
    for response_item in database.aql.execute(query=query, bind_vars=binds):
        new_node = DbMoNode.model_validate(response_item["node"])
        new_nodes.append(new_node)
        for edge_response_item in response_item["edges"]:
            new_edge = DbMoEdge.model_validate(edge_response_item)
            new_edges.append(new_edge)
    return new_nodes, new_edges
