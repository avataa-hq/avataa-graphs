from services.graph import GraphService
from services.inventory import InventoryInterface
from task.models.enums import Status
from task.models.incoming_data import PRM
from updater.updater_parts.prm_updater_parts.create_prm import create_prm
from updater.updater_parts.prm_updater_parts.delete_prm import delete_prm
from updater.updater_parts.prm_updater_parts.update_prm import update_prm
from updater.updater_parts.updater_abstract import ItemUpdaterAbstract
from updater.updater_parts.updater_task_abstract import UpdaterTaskAbstract


class PrmGraphUpdater(ItemUpdaterAbstract, UpdaterTaskAbstract):
    IGNORE_STATUS: set[Status] = {Status.NEW, Status.ERROR, Status.IN_PROCESS}

    def __init__(
        self, graph_db: GraphService, key: str, inventory: InventoryInterface
    ):
        ItemUpdaterAbstract.__init__(self)
        UpdaterTaskAbstract.__init__(self, graph_db=graph_db, key=key)
        self._inventory = inventory

    def _update(self, items: list[PRM]):
        response = update_prm(task=self, items=items, inventory=self._inventory)
        if response.update:
            self._update(items=response.update)
        if response.delete:
            self._delete(items=response.delete)
        if response.create:
            self._create(items=response.create)

    def _delete(self, items: list[PRM]):
        response = delete_prm(task=self, items=items)
        if response.update:
            self._update(items=response.update)
        if response.delete:
            self._delete(items=response.delete)
        if response.create:
            self._create(items=response.create)

    def _create(self, items: list[PRM]):
        response = create_prm(task=self, items=items, inventory=self._inventory)
        if response.update:
            self._update(items=response.update)
        if response.delete:
            self._delete(items=response.delete)
        if response.create:
            self._create(items=response.create)
