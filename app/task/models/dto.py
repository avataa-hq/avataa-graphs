from datetime import datetime
from typing import Any

from pydantic import BaseModel, Field
import pytz

from task.models.enums import ConnectionType, LinkType, Status
from task.models.incoming_data import MO, PRM, TMO, InitialRecordCreate


# Before DB
class ParsedValueTriggers(BaseModel):
    mos: list[int] = Field(default_factory=list)
    prms: list[int] = Field(default_factory=list)


class ParsedValue(BaseModel):
    raw_value: Any
    value: Any
    triggers: ParsedValueTriggers


class PrmDto(PRM):
    parsed_value: ParsedValue | None = None


class MoDto(MO):
    params: list[PrmDto] = Field(default_factory=lambda: [])


class GraphRecord(BaseModel):
    name: str
    tmo_id: int
    status: Status
    database: str = Field(..., min_length=1)
    active_tmo_ids: list[int] = Field(default_factory=lambda: [])
    error_description: str | None = None


class TmoEdge(BaseModel):
    from_: str = Field(..., alias="_from")
    to_: str = Field(..., alias="_to")
    link_type: LinkType
    enabled: bool
    tprm_id: int | None = None


class TmoNode(TMO):
    commutation_tprms: list[int] | None = None
    show_as_a_table: bool = Field(True)
    busy_parameter_groups: list[list[int]] = Field(default_factory=list)


class MoNode(BaseModel, validate_assignment=True):
    grouped_by_tprm: int | None = None
    group_p_id: int | None = None
    name: str
    label: str | None = None
    tmo: int
    mo_ids: list[int]
    is_trace: bool
    data: MoDto | None
    indexed: list[str] | None = None
    breadcrumbs: str = Field(default="/", pattern=r"^\/(.+\/)*$")


class MoEdge(BaseModel):
    from_: str = Field(..., alias="_from")
    to_: str = Field(..., alias="_to")
    connection_type: ConnectionType
    prm: list[int] | None = None
    tprm: int | None = None
    is_trace: bool
    virtual: bool
    source_id: str | None = None


# After DB
class ArangoBase(BaseModel):
    id: str = Field(..., alias="_id")
    key: str = Field(..., alias="_key")
    rev: str = Field(..., alias="_rev")


class DbGraphRecord(ArangoBase, GraphRecord):
    pass


class DbTmoEdge(ArangoBase, TmoEdge):
    pass


class DbTmoNode(ArangoBase, TmoNode):
    pass


class DbMoEdge(ArangoBase, MoEdge):
    pass


class DbMoNode(ArangoBase, MoNode):
    pass


# compose
class DbTmoNodeEdge(BaseModel):
    node: DbTmoNode
    edge: DbTmoEdge | None = None


class MainRecord(BaseModel):
    name: str
    tmo_id: int
    status: Status
    database: str = Field(..., min_length=1)
    active_tmo_ids: list[int] = Field(default_factory=lambda: [])
    error_description: str | None = None
    tmo_datetime: datetime | None = Field(default_factory=datetime.now)
    mo_datetime: datetime | None = Field(default_factory=datetime.now)


class DbMainRecord(ArangoBase, MainRecord):
    pass


class InitialRecordCreating(InitialRecordCreate):
    status: Status
    error_description: str | None = None
    database: str | None = None
    id: str | None = Field(None, alias="_id")
    key: str | None = Field(None, alias="_key")
    active_tmo_ids: list[int] | None = Field(None)
    created_at: datetime = Field(
        default_factory=lambda: datetime.utcnow().replace(tzinfo=pytz.UTC)
    )


class Path(BaseModel):
    nodes: list[DbMoNode] = Field(alias="vertices")
    edges: list[DbMoEdge] = Field(default_factory=list)
    tmo: list[DbTmoNode] = Field(default_factory=list)
    length: int = Field(..., alias="weight")
