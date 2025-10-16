from typing import Annotated, Any

from pydantic import (
    AliasChoices,
    AliasPath,
    BaseModel,
    BeforeValidator,
    ConfigDict,
    Field,
    computed_field,
)

from task.models.dto import InitialRecordCreating
from task.models.enums import Status
from task.models.incoming_data import MO, PRM, InitialRecordCreate


def clean_key(key: str):
    if key is None:
        return
    split_char = "/"
    parts = key.split(split_char)
    if len(parts) > 1:
        return parts[1]
    return key


class TmoNodeUpdate(BaseModel):
    key: int
    enabled: bool


class TmoEdgeUpdate(BaseModel):
    key: int
    enabled: bool


class TmoUpdate(BaseModel):
    nodes: list[TmoNodeUpdate] | None = None
    edges: list[TmoEdgeUpdate] | None = None
    group_by_tprms: list[int] | None = None
    start_from_tmo_id: int | None = None
    start_from_tprm_id: int | None = None
    trace_tmo_id: int | None = None
    trace_tprm_id: int | None = None
    delete_orphan_branches: bool | None = False


class TprmResponse(BaseModel):
    name: str
    val_type: str
    id: int


class TmoNodeResponse(BaseModel):
    key: str = Field(..., alias="_key")
    name: str
    virtual: bool
    global_uniqueness: bool
    id: int
    materialize: bool
    enabled: bool
    is_grouped: bool = Field(False)
    icon: str | None = None
    geometry_type: str | None = None
    line_type: str | None = None
    params: list[TprmResponse] = Field(default_factory=list)
    commutation_tprms: list[int] | None = None
    show_as_a_table: bool = Field(True)
    busy_parameter_groups: list[list[int]] = Field(default_factory=list)


class TmoEdgeResponse(BaseModel):
    key: str = Field(..., alias="_key")
    source: Annotated[str, BeforeValidator(clean_key)] = Field(
        ..., alias="_from"
    )
    target: Annotated[str, BeforeValidator(clean_key)] = Field(..., alias="_to")
    link_type: str
    enabled: bool
    tprm_id: int | None = Field(None)


class TmoConfigResponse(BaseModel):
    start_node_key: Annotated[str, BeforeValidator(clean_key)]
    nodes: list[TmoNodeResponse]
    edges: list[TmoEdgeResponse]
    group_by_tprms: list[int] | None
    start_from_tmo_id: int
    start_from_tprm_id: int | None = None
    trace_tmo_id: int | None = None
    trace_tprm_id: int | None = None
    delete_orphan_branches: bool = False


class InitialRecord(InitialRecordCreating):
    id: str = Field(..., alias="_id")
    key: str = Field(..., alias="_key")

    model_config = ConfigDict(frozen=True, populate_by_name=True)


class InitialRecordResponse(InitialRecordCreate):
    status: Status
    error_description: str | None = None
    key: str | None = Field(..., alias="_key")

    model_config = ConfigDict(populate_by_name=True)


class PrmResponse(PRM):
    value: Any = Field(
        None,
        validation_alias=AliasChoices(
            AliasPath("parsed_value", "value"), "value"
        ),
    )


class MoDto(MO):
    params: list[PrmResponse] = Field(default_factory=lambda: [])


class MoNodeResponse(BaseModel):
    key: str = Field(..., alias="_key")
    grouped_by_tprm: int | None
    name: str
    label: str | None = None
    tmo: int
    mo_ids: list[int]
    data: MoDto | None = None
    breadcrumbs: str = Field(default="/", pattern=r"^\/(.+\/)*$")
    connected_with: list[list[str]] | None = None


class MoEdgeResponse(BaseModel):
    key: str = Field(..., alias="_key")
    source: Annotated[str, BeforeValidator(clean_key)] = Field(
        ..., alias="_from"
    )
    target: Annotated[str, BeforeValidator(clean_key)] = Field(..., alias="_to")
    prm: list[int] | None = None
    tprm: int | None = None
    connection_type: str
    virtual: bool = Field(False)
    source_object: Annotated[str | None, BeforeValidator(clean_key)] = Field(
        None, alias="source_id"
    )


class CommutationResponse(BaseModel):
    tmo_id: int
    tmo_name: str
    parent_name: str
    parent_label: str | None = None
    nodes: list[MoNodeResponse]


class TmoResponse(BaseModel):
    name: str
    geometry_type: str | None = None
    tmo_id: int = Field(..., alias="id")
    icon: str | None = None
    line_type: str | None = None


class NodeEdgeErrorResponse(BaseModel):
    description: str
    params: dict


class NodeEdgeResponse(BaseModel):
    nodes: list[MoNodeResponse]
    edges: list[MoEdgeResponse]
    tmo: list[TmoResponse]


class NodeEdgeCommutationResponse(NodeEdgeResponse):
    commutation: list[CommutationResponse] | None = None

    @computed_field
    @property
    def size(self) -> int:
        size = len(self.nodes)
        if self.commutation:
            size += len(self.commutation)
        return size


class NodeTmoResponse(BaseModel):
    nodes: list[MoNodeResponse]
    tmo: list[TmoResponse]


class CollapseNodeResponse(BaseModel):
    collapse_from: list[MoNodeResponse]
    collapse_to: MoNodeResponse
    tmo: list[TmoResponse]


class TPRMResponse(BaseModel):
    name: str = Field(..., min_length=1)
    val_type: str = Field(..., min_length=1)
    tmo_id: int
    id: int


class NodeEdgeTmoTprmResponse(NodeEdgeResponse):
    tprm: list[TPRMResponse]


class PathResponse(NodeEdgeResponse):
    nodes: list[MoNodeResponse] = Field(default_factory=list)
    tmo: list[TmoResponse] = Field(default_factory=list)
    length: int = Field(..., validation_alias="weight")
