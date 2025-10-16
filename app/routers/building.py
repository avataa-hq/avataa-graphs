from sys import stderr
import traceback

from fastapi import APIRouter, BackgroundTasks, Depends, HTTPException, status

from services.instances import (
    build_graph_in_new_process,
    create_db_connection_instance,
    inventory,
)
from services.security.security_data_models import UserData
from services.security.security_factory import security
from task.building_tasks import RunBuildingTask
from task.models.errors import NotFound, ValidationError

router = APIRouter(prefix="/building", tags=["building"])


@router.post("/", status_code=status.HTTP_202_ACCEPTED)
def build(
    key: str,
    background: BackgroundTasks,
    user_data: UserData = Depends(security),
):
    graph_db = create_db_connection_instance()
    task = RunBuildingTask(graph_db=graph_db, inventory=inventory, key=key)
    try:
        task.check()
    except NotFound as e:
        print(traceback.format_exc(), file=stderr)
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND, detail=str(e)
        )
    except ValidationError as e:
        print(traceback.format_exc(), file=stderr)
        raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail=str(e))

    background.add_task(build_graph_in_new_process, key=key, daemon=True)
    return {"message": "started"}
