from __future__ import annotations

from arango import DocumentInsertError
from pydantic import BaseModel, Field

from task.models.errors import GraphBuildingError
from task.task_abstract import TaskAbstract


class UniqueFromToEdge(BaseModel):
    from_: str = Field(..., alias="_from")
    to_: str = Field(..., alias="_to")

    def __hash__(self):
        return hash(tuple(sorted((self.from_, self.to_))))

    def __eq__(self, other):
        return (
            isinstance(other, UniqueFromToEdge)
            and self.__hash__() == other.__hash__()
        )


def get_unique_connections(task: TaskAbstract) -> set[UniqueFromToEdge]:
    query = """
        FOR edge IN @@mainEdgeCollection
            FILTER edge.virtual == false
            FILTER edge.is_trace == false
            RETURN DISTINCT {"_from": edge._from, "_to": edge._to}
    """
    binds = {"@mainEdgeCollection": task.main_edge_collection.name}
    response = task.database.aql.execute(query=query, bind_vars=binds)
    unique_connections = set()
    for item in response:
        edge = UniqueFromToEdge.model_validate(item)
        unique_connections.add(edge)
    return unique_connections


def save_unique_connections(
    task: TaskAbstract, unique_connections: set[UniqueFromToEdge]
):
    unique_connections = [
        i.model_dump(mode="json", by_alias=True) for i in unique_connections
    ]
    for edge in task.main_path_collection.insert_many(
        unique_connections, keep_none=True
    ):
        if isinstance(edge, DocumentInsertError):
            raise GraphBuildingError(f"Edge insertion error. {str(edge)}")


def fill_path_edge_collection(task: TaskAbstract):
    unique_connections = get_unique_connections(task)
    save_unique_connections(unique_connections=unique_connections, task=task)
