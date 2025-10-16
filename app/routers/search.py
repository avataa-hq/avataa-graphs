from typing import Annotated

from fastapi import APIRouter, Depends, Query

from routers.helpers.try_catch_task_exception import try_catch_task_exception
from services.instances import graph_db, inventory
from services.security.security_data_models import UserData
from services.security.security_factory import security
from task.find_nodes_by_mo_id import FindNodesByMoId
from task.models.outgoing_data import MoNodeResponse, NodeTmoResponse
from task.models.trace_models import NodesByMoIdResponseItem
from task.search_tasks import FindInGraphTask, GetBreadcrumbsTask

router = APIRouter(prefix="/search", tags=["search"])


@router.get(
    "/hierarchy/{key}",
    response_model=list[MoNodeResponse],
    response_model_by_alias=False,
)
def get_hierarchy(
    key: str,
    node_key: Annotated[str, Query()],
    user_data: UserData = Depends(security),
) -> list[MoNodeResponse]:
    task = GetBreadcrumbsTask(graph_db=graph_db, key=key, node_key=node_key)
    return try_catch_task_exception(task)


@router.get(
    "/{key}",
    response_model=NodeTmoResponse,
    response_model_by_alias=False,
)
def search_by_value(
    key: str,
    query: Annotated[str, Query(min_length=3, max_length=36)],
    user_data: UserData = Depends(security),
) -> NodeTmoResponse:
    task = FindInGraphTask(graph_db=graph_db, key=key, find_value=query)
    return try_catch_task_exception(task)


@router.get(
    "/nodes_by_mo_id/{mo_id}",
    response_model=list[NodesByMoIdResponseItem],
    response_model_by_alias=False,
)
def get_nodes_by_mo_id(mo_id: int, user_data: UserData = Depends(security)):
    task = FindNodesByMoId(graph_db=graph_db, inventory=inventory, mo_id=mo_id)
    return try_catch_task_exception(task)
