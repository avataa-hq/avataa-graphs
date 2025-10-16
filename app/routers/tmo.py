from typing import Annotated

from fastapi import APIRouter, Body, Depends

from routers.helpers.try_catch_task_exception import try_catch_task_exception
from services.instances import graph_db
from services.security.security_data_models import UserData
from services.security.security_factory import security
from task.busy_parameters_task import SetBusyParametersTask
from task.commutation_tprms_task import SetCommutationTprmsTask
from task.models.outgoing_data import (
    TmoConfigResponse,
    TmoNodeResponse,
    TmoUpdate,
)
from task.show_as_table_task import ShowAsATableTask
from task.tmo_tasks import TmoTask, TmoUpdateTask

router = APIRouter(prefix="/tmo", tags=["tmo"])


@router.get(
    "/{key}", response_model=TmoConfigResponse, response_model_by_alias=False
)
def get_tmo_graph(key: str, user_data: UserData = Depends(security)):
    task = TmoTask(key=key, graph_db=graph_db)
    return try_catch_task_exception(task)


@router.patch(
    "/{key}/tprms",
    response_model=TmoNodeResponse,
    response_model_by_alias=False,
    deprecated=True,
)
def set_commutation_tprms(
    key: str,
    node_key: Annotated[str, Body(embed=True)],
    tprm_ids: Annotated[list[int] | None, Body(embed=True)] = None,
):
    task = SetCommutationTprmsTask(
        key=key, node_key=node_key, graph_db=graph_db, tprm_ids=tprm_ids
    )
    return try_catch_task_exception(task)


@router.patch(
    "/{key}/busy_parameters",
    response_model=TmoNodeResponse,
    response_model_by_alias=False,
    deprecated=True,
)
def set_busy_parameters(
    key: str,
    node_key: Annotated[str, Body(embed=True)],
    busy_parameters: Annotated[list[list[int]] | None, Body(embed=True)] = None,
):
    task = SetBusyParametersTask(
        key=key,
        node_key=node_key,
        graph_db=graph_db,
        busy_parameter_groups=busy_parameters,
    )
    return try_catch_task_exception(task)


@router.patch(
    "/{key}/show_as_a_table",
    response_model=TmoNodeResponse,
    response_model_by_alias=False,
)
def set_show_as_a_table(
    key: str,
    node_key: Annotated[str, Body(embed=True)],
    show_as_a_table: Annotated[bool, Body(embed=True)],
    user_data: UserData = Depends(security),
):
    task = ShowAsATableTask(
        key=key,
        node_key=node_key,
        show_as_a_table=show_as_a_table,
        graph_db=graph_db,
    )
    return try_catch_task_exception(task)


@router.patch(
    "/{key}", response_model=TmoConfigResponse, response_model_by_alias=False
)
def update_tmo_graph(
    key: str, tmo_data: TmoUpdate, user_data: UserData = Depends(security)
):
    task = TmoUpdateTask(key=key, data=tmo_data, graph_db=graph_db)
    return try_catch_task_exception(task)
