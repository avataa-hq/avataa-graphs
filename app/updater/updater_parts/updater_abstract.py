from abc import ABC, abstractmethod
from enum import Enum

from task.models.enums import Status


class OperationType(Enum):
    UPDATED = "updated"
    DELETED = "deleted"
    CREATED = "created"


class ObjType(Enum):
    TMO = "TMO"
    TPRM = "TPRM"
    MO = "MO"
    PRM = "PRM"


class ItemUpdaterAbstract(ABC):
    IGNORE_STATUS: set[Status] = set()

    def update_data(
        self, status: Status, operation: OperationType, items: list
    ):
        if status in self.IGNORE_STATUS:
            return
        match operation:
            case OperationType.UPDATED:
                self._update(items=items)
            case OperationType.CREATED:
                self._create(items=items)
            case OperationType.DELETED:
                self._delete(items=items)

    @abstractmethod
    def _update(self, items: list):
        raise NotImplementedError()

    @abstractmethod
    def _delete(self, items: list):
        raise NotImplementedError()

    @abstractmethod
    def _create(self, items: list):
        raise NotImplementedError()
