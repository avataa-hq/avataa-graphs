from typing import Annotated

from fastapi import APIRouter, Depends, Path

from routers.helpers.try_catch_task_exception import try_catch_task_exception
from services.instances import graph_db
from services.security.security_data_models import UserData
from services.security.security_factory import security
from task.tmp_get_mo_info_task import TmpGtMoInfoTask

router = APIRouter(prefix="/tmp", tags=["tmp"])  # noqa: S108


@router.get("/{key}/{mo_id}")
def get_mo_info(
    key: str,
    mo_id: Annotated[int, Path(gt=0)],
    user_data: UserData = Depends(security),
):
    task = TmpGtMoInfoTask(key=key, graph_db=graph_db, mo_id=mo_id)
    return try_catch_task_exception(task)
