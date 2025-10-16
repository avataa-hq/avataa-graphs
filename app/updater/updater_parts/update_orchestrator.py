from copy import deepcopy
from multiprocessing import Value
from time import sleep

from services.graph import GraphService
from services.inventory import InventoryInterface
from task.models.dto import DbTmoNode
from task.models.enums import Status
from updater.converters.inventory.inventory_changes_topic import ParsedMessage
from updater.kafka_listener import TopicSubscriber
from updater.updater_parts.mo_updater import MoGraphUpdater
from updater.updater_parts.prm_updater import PrmGraphUpdater
from updater.updater_parts.tmo_updater import (
    TmoMainUpdater,
    TmoSettingsUpdater,
    TmoTmoUpdater,
)
from updater.updater_parts.tprm_updater import (
    TprmSettingUpdater,
    TprmTmoUpdater,
)
from updater.updater_parts.updater_abstract import (
    ItemUpdaterAbstract,
    ObjType,
    OperationType,
)
from updater.updater_parts.updater_task_abstract import UpdaterTaskAbstract


class DatabaseTmoCache(UpdaterTaskAbstract):
    def __init__(
        self,
        graph_db: GraphService,
        key: str,
    ):
        UpdaterTaskAbstract.__init__(self, graph_db=graph_db, key=key)
        self.tmo: dict[int, bool] = {}  # {tmo_id: enabled}
        self.tprm: dict[int, int] = {}  # {tprm_id: tmo_id}
        # TODO Добавить mo и prm из ссылочных типов
        self.__init_tmo()

    def __init_tmo(self):
        for result in self.tmo_collection.all():
            result = DbTmoNode.model_validate(result)
            self.tmo[result.tmo_id] = (
                True if result.tmo_id == self.trace_tmo_id else result.enabled
            )
            if result.params:
                for param in result.params:
                    self.tprm[param.id] = param.tmo_id

    def update_data(self, *args, **kwargs):
        self.__init_tmo()

    def update_cache_before(
        self,
        obj_type: ObjType,
        operation: OperationType,
        message: ParsedMessage,
    ):
        if operation == OperationType.CREATED:
            if obj_type == ObjType.TMO:
                for message_item in message.value:
                    if message_item.p_id in self.tmo:
                        self.tmo[message_item.tmo_id] = False
            elif obj_type == ObjType.TPRM:
                for message_item in message.value:
                    if message_item.tmo_id in self.tmo:
                        self.tprm[message_item.id] = message_item.tmo_id
        elif operation == OperationType.UPDATED:
            if obj_type == ObjType.TMO:
                for message_item in message.value:
                    if message_item.p_id in self.tmo:
                        self.tmo[message_item.tmo_id] = False

    def update_cache_after(
        self,
        obj_type: ObjType,
        operation: OperationType,
        message: ParsedMessage,
    ):
        if operation == OperationType.DELETED:
            if obj_type == ObjType.TMO:
                for message_item in message.value:
                    self.tmo.pop(message_item.tmo_id, None)
            elif obj_type == ObjType.TPRM:
                for message_item in message.value:
                    self.tprm.pop(message_item.id, None)

    def filter(self, obj_type: ObjType, message: ParsedMessage):
        if obj_type == ObjType.TMO:
            value = [i for i in message.value if i.tmo_id in self.tmo]
        elif obj_type == ObjType.TPRM:
            value = [i for i in message.value if i.id in self.tprm]
        elif obj_type == ObjType.MO:
            value = [i for i in message.value if i.tmo_id in self.tmo]
        elif obj_type == ObjType.PRM:
            value = [i for i in message.value if i.tprm_id in self.tprm]
        else:
            raise NotImplementedError(f"Unsupported object type {obj_type}")
        return ParsedMessage(key=message.key, value=deepcopy(value))


class UpdateOrchestrator(TopicSubscriber, DatabaseTmoCache):
    def __init__(
        self,
        topic: str,
        graph_db: GraphService,
        inventory: InventoryInterface,
        database: str,
        status: Value,
    ):
        TopicSubscriber.__init__(self)
        DatabaseTmoCache.__init__(self, graph_db=graph_db, key=database)
        self._topic = topic
        self._inventory = inventory
        self.status = status
        self.updaters: dict[ObjType, list[ItemUpdaterAbstract]] = {
            ObjType.TMO: [
                TmoMainUpdater(graph_db=graph_db, key=database),
                TmoSettingsUpdater(graph_db=graph_db, key=database),
                TmoTmoUpdater(
                    graph_db=graph_db, key=database, inventory=inventory
                ),
                self,  # так проще обновлять кеш
            ],
            ObjType.TPRM: [
                TprmSettingUpdater(graph_db=graph_db, key=database),
                TprmTmoUpdater(
                    graph_db=graph_db, key=database, inventory=inventory
                ),
            ],
            ObjType.MO: [
                MoGraphUpdater(
                    graph_db=graph_db, key=database, inventory=inventory
                ),
            ],
            ObjType.PRM: [
                PrmGraphUpdater(
                    graph_db=graph_db, key=database, inventory=inventory
                ),
            ],
        }
        self.sep = ":"

    def send_message(self, message: ParsedMessage):
        status: Status = list(Status)[self.status.value]
        while status == Status.IN_PROCESS:
            sleep(5)

        obj_type, operation = message.key.split(":", 1)
        obj_type = ObjType(obj_type)
        operation = OperationType(operation)
        self.update_cache_before(
            obj_type=obj_type, operation=operation, message=message
        )
        filtered_message = self.filter(obj_type=obj_type, message=message)
        self.update_cache_after(
            obj_type=obj_type, operation=operation, message=message
        )

        if not filtered_message.value:
            return

        updaters_list = self.updaters[obj_type]
        print(f"{operation=}\n{filtered_message.value=}\n{status=}")
        for updater in updaters_list:
            updater.update_data(
                operation=operation, items=filtered_message.value, status=status
            )
