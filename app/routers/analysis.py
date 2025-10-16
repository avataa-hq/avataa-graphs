from typing import Annotated

from fastapi import APIRouter, Body, Depends

from routers.helpers.try_catch_task_exception import try_catch_task_exception
from services.instances import graph_db
from services.security.security_data_models import UserData
from services.security.security_factory import security
from task.analysis_tasks import (
    CollapseNodesTask,
    ExpandEdgesTask,
    ExpandNodesTask,
    GetNeighborsTask,
    GetTopLevelAnalysisTask,
)
from task.edges_between_nodes_task import FindEdgesBetweenNodesTask
from task.models.outgoing_data import (
    CollapseNodeResponse,
    NodeEdgeCommutationResponse,
    NodeEdgeTmoTprmResponse,
)

router = APIRouter(prefix="/analysis", tags=["analysis"])


@router.post(
    "/top_level/{key}",
    response_model=NodeEdgeCommutationResponse,
    response_model_by_alias=False,
)
def get_top_level(
    key: str,
    max_size: Annotated[
        int, Body(description="Maximum number of nodes", embed=True, ge=0)
    ] = 0,
    user_data: UserData = Depends(security),
):
    task = GetTopLevelAnalysisTask(
        graph_db=graph_db, key=key, max_size=max_size
    )
    return try_catch_task_exception(task)


@router.post(
    "/expand/{key}",
    response_model=NodeEdgeCommutationResponse,
    response_model_by_alias=False,
)
def expand(
    key: str,
    node_key: Annotated[str, Body()],
    neighboring_node_keys: Annotated[list[str], Body()],
    max_size: Annotated[
        int, Body(description="Maximum number of nodes", ge=0)
    ] = 0,
    return_commutation_label: Annotated[bool, Body()] = False,
    expand_edges: Annotated[bool, Body()] = False,
    user_data: UserData = Depends(security),
):
    task = ExpandNodesTask(
        graph_db=graph_db,
        key=key,
        node_key=node_key,
        neighboring_node_keys=neighboring_node_keys,
        max_size=max_size,
        return_commutation_label=return_commutation_label,
        expand_edges=expand_edges,
    )
    return try_catch_task_exception(task)


@router.post(
    "/collapse/{key}",
    response_model=CollapseNodeResponse,
    response_model_by_alias=False,
)
def collapse(
    key: str,
    node_key: Annotated[str, Body(embed=True)],
    user_data: UserData = Depends(security),
):
    task = CollapseNodesTask(graph_db=graph_db, key=key, node_key=node_key)
    return try_catch_task_exception(task)


@router.post(
    "/expand_edge/{key}",
    response_model=NodeEdgeTmoTprmResponse,
    response_model_by_alias=False,
    deprecated=True,
)
def expand_edge(
    key: str,
    node_key_a: Annotated[str, Body(embed=True)],
    node_key_b: Annotated[str, Body(embed=True)],
):
    task = ExpandEdgesTask(
        graph_db=graph_db, key=key, node_key_a=node_key_a, node_key_b=node_key_b
    )
    return try_catch_task_exception(task)


@router.post(
    "/neighbors/{key}",
    response_model=NodeEdgeCommutationResponse,
    response_model_by_alias=False,
)
def get_neighbors(
    key: str,
    node_key: Annotated[str, Body(embed=True)],
    n: Annotated[int, Body(embed=True, ge=1)],
    with_all_edges: Annotated[bool, Body(embed=True)] = False,
    user_data: UserData = Depends(security),
):
    task = GetNeighborsTask(
        graph_db=graph_db,
        key=key,
        node_key=node_key,
        n=n,
        with_all_edges=with_all_edges,
    )
    return try_catch_task_exception(task)


@router.post(
    "/edges_between_nodes/{key}",
    response_model=NodeEdgeTmoTprmResponse,
    response_model_by_alias=False,
)
def edges_between_nodes(
    key: str,
    node_keys: Annotated[list[str], Body()],
    user_data: UserData = Depends(security),
):
    task = FindEdgesBetweenNodesTask(
        graph_db=graph_db, key=key, node_keys=node_keys
    )
    return try_catch_task_exception(task=task)
