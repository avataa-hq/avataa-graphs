from fastapi import APIRouter, BackgroundTasks, Depends, HTTPException, status

from services.instances import graph_db, inventory
from services.security.security_data_models import UserData
from services.security.security_factory import security
from task.initialisation_tasks import (
    DeleteGraphStateTask,
    GraphStatesTask,
    GraphStateUpdateTask,
    InitGraphTask,
)
from task.models.incoming_data import InitialRecordCreate, InitialRecordUpdate
from task.models.outgoing_data import InitialRecordResponse

router = APIRouter(prefix="/initialisation", tags=["initialisation"])


@router.post("/", status_code=status.HTTP_202_ACCEPTED)
def create_general_data(
    data: InitialRecordCreate,
    background: BackgroundTasks,
    user_data: UserData = Depends(security),
):
    """
    Adds new graph data. The first stage in constructing a graph
    :param data: The name of the future graph, the starting point for constructing a preliminary graph in the form
    of a TMO
    :type data: InitialRecordCreate
    :param background: task scheduler in background
    :type background: BackgroundTasks
    :param user_data: Information about User
    :type user_data: UserData
    :return: {"message": "started"} if user data passes checks OR error if not passes
    :rtype: dict
    """
    task = InitGraphTask(
        graph_data=data, graph_db=graph_db, inventory=inventory
    )
    try:
        task.check()
    except ValueError as e:
        raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail=str(e))

    background.add_task(task.execute)
    return {"message": "started"}


@router.get(
    "/",
    response_model=list[InitialRecordResponse],
    status_code=status.HTTP_200_OK,
    response_model_by_alias=False,
)
def get_general_data(user_data: UserData = Depends(security)):
    """
    Obtaining the list of created graphs with statuses, and may also contain an error
    :param user_data: Information about User
    :type user_data: UserData
    :return: List of created graphs
    :rtype: list[InitialRecordResponse]
    """
    task = GraphStatesTask(graph_db=graph_db)
    results = task.execute()
    return results


@router.patch(
    "/{key}",
    response_model=InitialRecordResponse,
    status_code=status.HTTP_200_OK,
    response_model_by_alias=False,
)
def update_general_data(
    key: str, data: InitialRecordUpdate, user_data: UserData = Depends(security)
):
    """
    Updating previously created graph information
    :param key: key of the graph to update
    :type key: str
    :param data: data to update
    :type data: InitialRecordUpdate
    :param user_data: Information about User
    :type user_data: UserData
    :return: updated data
    :rtype: InitialRecordResponse
    """
    task = GraphStateUpdateTask(graph_db=graph_db, key=key, data=data)
    try:
        task.check()
    except ValueError as e:
        raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail=str(e))
    response = task.execute()
    return response


@router.delete("/{key}", status_code=status.HTTP_204_NO_CONTENT)
def delete_general_data(
    key: str,
    user_data: UserData = Depends(security),
):
    """
    Delete previously created graph and graph information
    :param key: key of the graph to update
    :type key: str
    :param user_data: Information about User
    :type user_data: UserData
    :return: Null
    :rtype: None
    """
    task = DeleteGraphStateTask(graph_db=graph_db, key=key)
    try:
        task.check()
    except ValueError as e:
        raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail=str(e))
    task.execute()
