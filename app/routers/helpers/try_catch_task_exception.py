from sys import stderr
import traceback

from fastapi import HTTPException
from starlette.status import (
    HTTP_404_NOT_FOUND,
    HTTP_408_REQUEST_TIMEOUT,
    HTTP_409_CONFLICT,
)

from task.models.errors import NotFound, TimeOutError, ValidationError


def try_catch_task_exception(task):
    try:
        task.check()
        return task.execute()
    except NotFound as e:
        print(traceback.format_exc(), file=stderr)
        raise HTTPException(status_code=HTTP_404_NOT_FOUND, detail=str(e))
    except TimeOutError as e:
        print(traceback.format_exc(), file=stderr)
        raise HTTPException(status_code=HTTP_408_REQUEST_TIMEOUT, detail=str(e))
    except ValidationError as e:
        print(traceback.format_exc(), file=stderr)
        raise HTTPException(status_code=HTTP_409_CONFLICT, detail=str(e))
