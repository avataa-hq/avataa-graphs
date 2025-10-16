from __future__ import annotations

from datetime import datetime
import json
from typing import Annotated, Any

from pydantic import (
    AliasChoices,
    BaseModel,
    BeforeValidator,
    Field,
    model_validator,
)


def convert_str_to_dict(inp: None | str | dict | list) -> dict | list | None:
    if inp is None or inp == "":
        return None
    if isinstance(inp, str):
        return json.loads(inp)
    return inp


def int_grpc_to_none(inp: None | int) -> int | None:
    if inp is None:
        return None
    if inp == "0" or inp == 0:
        return None
    return inp


def str_grpc_to_none(inp: None | int) -> int | None:
    if inp is None:
        return None
    if inp == "":
        return None
    return inp


class TPRM(BaseModel):
    name: str = Field(..., min_length=1)
    val_type: str = Field(..., min_length=1)
    required: bool
    returnable: bool
    tmo_id: int
    id: int
    multiple: bool
    description: str | None = None
    constraint: list[int] | str | None = None
    prm_link_filter: str | None = None
    group: str | None = None
    created_by: str | None = None
    modified_by: str | None = None
    creation_date: datetime | None = None
    modification_date: datetime | None = None
    version: int | None = None
    field_value: str | None = None


class TMO(BaseModel):
    p_id: Annotated[int | None, BeforeValidator(int_grpc_to_none)] = Field(
        None, gt=0
    )
    name: str = Field(..., min_length=1)
    icon: str | None = None
    description: str | None = None
    virtual: bool
    global_uniqueness: bool
    tmo_id: int = Field(..., alias="id")
    materialize: bool
    points_constraint_by_tmo: list[int] | None = Field(
        None,
        validation_alias=AliasChoices(
            "point_tmo_const", "points_constraint_by_tmo"
        ),
        alias="point_tmo_const",
    )
    enabled: bool = True
    minimize: bool = False
    geometry_type: str | None = None
    line_type: str | None = None
    label: list[int] = Field(default_factory=list)
    params: list[TPRM] | None = Field(default_factory=list)


class PRM(BaseModel):
    tprm_id: int
    mo_id: int
    value: Any
    id: int
    version: int


class MO(BaseModel):
    tmo_id: int
    p_id: Annotated[int | None, BeforeValidator(int_grpc_to_none)] = Field(
        None, gt=0
    )
    id: int
    name: str
    label: str | None = None
    active: bool
    version: int
    latitude: float | None = Field(None)
    longitude: float | None = Field(None)
    pov: Annotated[dict | list | None, BeforeValidator(convert_str_to_dict)] = (
        None
    )
    geometry: Annotated[
        dict | list | None, BeforeValidator(convert_str_to_dict)
    ] = None
    model: Annotated[str | None, BeforeValidator(str_grpc_to_none)] = Field(
        None, min_length=1
    )
    point_a_id: Annotated[int | None, BeforeValidator(int_grpc_to_none)] = (
        Field(None, gt=0)
    )
    point_b_id: Annotated[int | None, BeforeValidator(int_grpc_to_none)] = (
        Field(None, gt=0)
    )
    status: Annotated[str | None, BeforeValidator(str_grpc_to_none)] = Field(
        None, min_length=1
    )
    params: list[PRM] = Field(default_factory=lambda: [])

    @model_validator(mode="after")
    def coords(self):
        if self.latitude == 0 and self.latitude == 0:
            self.longitude = None
            self.latitude = None
        elif (self.latitude is None) != (self.longitude is None):
            raise ValueError(
                "Latitude and longitude cannot be specified inconsistently"
            )
        return self


class InitialRecordCreate(BaseModel):
    name: str = Field(..., min_length=1)
    tmo_id: int = Field(..., gt=0, lt=2_147_483_647)


class InitialRecordUpdate(BaseModel):
    name: str = Field(..., min_length=1)
