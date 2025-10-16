from pydantic import BaseModel

from task.models.outgoing_data import MoNodeResponse


class NodesByMoIdResponseItem(BaseModel):
    key: str
    name: str
    nodes: list[MoNodeResponse]
