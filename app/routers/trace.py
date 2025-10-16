from typing import Annotated

from fastapi import APIRouter, Body, Depends

from routers.helpers.try_catch_task_exception import try_catch_task_exception
from services.instances import graph_db, inventory
from services.security.security_data_models import UserData
from services.security.security_factory import security
from task.find_trace_nodes_by_mo_id import (
    FindTraceNodesByMoId,
    FindTraceNodesByMoIds,
)
from task.find_way_between_a_b import FindPathBetweenNodesTask
from task.models.outgoing_data import (
    MoNodeResponse,
    NodeEdgeResponse,
    PathResponse,
)
from task.models.trace_models import NodesByMoIdResponseItem
from task.trace_levels_task import TrackingType
from task.trace_tasks import FindCommonPath, GetAllPathsForNodeTask, GetPathTask

router = APIRouter(prefix="/trace", tags=["trace"])


@router.post(
    "/path/all/{key}",
    response_model=list[MoNodeResponse],
    response_model_by_alias=False,
)
def get_all_paths_for_node(
    key: str,
    node_key: Annotated[str, Body(embed=True)],
    user_data: UserData = Depends(security),
):
    task = GetAllPathsForNodeTask(key=key, node_key=node_key, graph_db=graph_db)
    return try_catch_task_exception(task)


@router.post(
    "/path/{key}",
    response_model=NodeEdgeResponse,
    response_model_by_alias=False,
)
def get_path(
    key: str,
    trace_node_key: Annotated[str, Body(embed=True)],
    squash_level: Annotated[TrackingType, Body(embed=True)],
    user_data: UserData = Depends(security),
):
    task = GetPathTask(
        graph_db=graph_db,
        key=key,
        trace_node_key=trace_node_key,
        level=squash_level,
    )
    return try_catch_task_exception(task)


@router.post(
    "/nodes_by_mo_id/{mo_id}",
    response_model=list[NodesByMoIdResponseItem],
    response_model_by_alias=False,
    deprecated=True,
)
def get_nodes_by_mo_id(mo_id: int):
    task = FindTraceNodesByMoId(
        graph_db=graph_db, inventory=inventory, mo_id=mo_id
    )
    return try_catch_task_exception(task)


@router.post(
    "/nodes_by_mo_ids",
    response_model=list[NodesByMoIdResponseItem],
    response_model_by_alias=False,
)
def get_nodes_by_mo_ids(
    mo_ids: Annotated[list[int], Body(embed=True)],
    user_data: UserData = Depends(security),
):
    task = FindTraceNodesByMoIds(
        graph_db=graph_db, inventory=inventory, mo_ids=mo_ids
    )
    return try_catch_task_exception(task)


@router.post(
    "/path_between_nodes/{key}",
    response_model=list[PathResponse],
    response_model_by_alias=False,
)
def get_path_between_nodes(
    key: str,
    node_key_a: str,
    node_key_b: str,
    squash_level: Annotated[
        TrackingType, Body(embed=True)
    ] = TrackingType.LOCAL,
    user_data: UserData = Depends(security),
):
    task = FindPathBetweenNodesTask(
        graph_db=graph_db,
        key=key,
        node_key_a=node_key_a,
        node_key_b=node_key_b,
        level=squash_level,
    )
    return try_catch_task_exception(task)


@router.post(
    "/find_common_path/{key}",
    response_model=NodeEdgeResponse,
    response_model_by_alias=False,
)
def find_common_path(
    key: str,
    trace_node_a_key: Annotated[str, Body(embed=True)],
    trace_node_b_key: Annotated[str, Body(embed=True)],
    squash_level: Annotated[TrackingType, Body(embed=True)],
    user_data: UserData = Depends(security),
):
    task = FindCommonPath(
        graph_db=graph_db,
        key=key,
        trace_node_a_key=trace_node_a_key,
        trace_node_b_key=trace_node_b_key,
        level=squash_level,
    )
    return try_catch_task_exception(task)
