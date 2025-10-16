from services.graph import GraphService
from services.inventory import InventoryInterface
from task.models.enums import Status
from updater.updater_parts.mo_updater_parts.create_mo import mo_create
from updater.updater_parts.mo_updater_parts.delete_mo import mo_delete
from updater.updater_parts.mo_updater_parts.update_mo import mo_update
from updater.updater_parts.updater_abstract import ItemUpdaterAbstract
from updater.updater_parts.updater_task_abstract import UpdaterTaskAbstract


class MoGraphUpdater(ItemUpdaterAbstract, UpdaterTaskAbstract):
    IGNORE_STATUS: set[Status] = {Status.NEW, Status.ERROR, Status.IN_PROCESS}

    def __init__(
        self, graph_db: GraphService, key: str, inventory: InventoryInterface
    ):
        ItemUpdaterAbstract.__init__(self)
        UpdaterTaskAbstract.__init__(self, graph_db=graph_db, key=key)
        self._inventory = inventory

    def _update(self, items: list):
        response = mo_update(task=self, items=items)
        if response.create:
            self._create(items=response.create)
        if response.delete:
            self._delete(items=response.delete)

    def _delete(self, items: list):
        response = mo_delete(task=self, items=items)
        if response.update:
            self._update(items=response.update)
        if response.create:
            self._create(items=response.create)

    def _create(self, items: list):
        response = mo_create(task=self, items=items, inventory=self._inventory)
        if response.update:
            self._update(items=response.update)
        if response.delete:
            self._delete(items=response.delete)
