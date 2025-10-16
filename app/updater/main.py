from collections import namedtuple
import datetime
from multiprocessing import Lock, Process, Value
from time import sleep

from config import ArangoConfig, GraphDBConfig, InventoryGRPCConfig
from services.graph import GraphService
from services.inventory import Inventory, InventoryInterface
from task.models.dto import DbMainRecord
from task.models.enums import Status
from updater.converters.inventory.inventory_changes_topic import TopicConverter
from updater.kafka_listener import KafkaListener
from updater.updater_config import KafkaTopicsConfig
from updater.updater_parts.update_orchestrator import UpdateOrchestrator

GraphState = namedtuple("GraphState", ["status", "worker", "status_index"])
GraphStateDiff = namedtuple("GraphStateDiff", ["updated", "created", "deleted"])


def get_new_graph_db(config: ArangoConfig | None = None) -> GraphService:
    if config is None:
        config = ArangoConfig()
    return GraphService(
        url=config.url,
        username=config.username,
        password=config.password,
        sys_database_name=GraphDBConfig().sys_database_name,
    )


def get_new_inventory(
    multiprocessing_lock: Lock, config: InventoryGRPCConfig | None = None
) -> InventoryInterface:
    if config is None:
        config = InventoryGRPCConfig()
    return Inventory(config.url, lock=multiprocessing_lock)


def new_worker(database: str, status: Value, multiprocessing_lock: Lock):
    topic = KafkaTopicsConfig().inventory
    converter = TopicConverter(topic=topic)
    listener = KafkaListener(group_postfix=database)
    listener.add_topic_converter(converter=converter)
    graph_db_instance = get_new_graph_db()
    inventory_instance = get_new_inventory(
        multiprocessing_lock=multiprocessing_lock
    )
    subscriber = UpdateOrchestrator(
        topic=topic,
        database=database,
        graph_db=graph_db_instance,
        inventory=inventory_instance,
        status=status,
    )
    listener.subscribe(topic=topic, subscriber=subscriber)
    listener.start()


class MainUpdateOrchestrator:
    def __init__(
        self, graph_db: GraphService | None, update_period_s: int = 60
    ):
        self.multiprocessing_lock = Lock()
        self.graph_db = graph_db if graph_db else get_new_graph_db()
        self.config = GraphDBConfig()
        self.main_collection = self.graph_db.get_collection(
            self.graph_db.sys_db, self.config.main_graph_collection_name
        )
        self.graph_state: dict[str, GraphState] = {}
        self.update_period_s = update_period_s
        self.next_update = datetime.datetime.now()

    def get_state(self) -> dict[str, Status]:
        results = {}
        for main_record in self.main_collection.all():
            main_record = DbMainRecord.model_validate(main_record)
            state = main_record.status
            results[main_record.key] = state
        return results

    def tick(self):
        now = datetime.datetime.now()
        if now <= self.next_update:
            delta = (self.next_update - now).total_seconds()
            sleep(delta)
        self.next_update = now + datetime.timedelta(
            seconds=self.update_period_s
        )

    def get_state_diff(self, state: dict[str, Status]) -> GraphStateDiff:
        new_db = set(state.keys()).difference(self.graph_state.keys())
        old_db = set(self.graph_state.keys()).difference(state.keys())
        update_db = set()
        for db_id in set(state).intersection(self.graph_state.keys()):
            prev_state = self.graph_state[db_id]
            next_state = state[db_id]
            if prev_state.status != next_state:
                update_db.add(db_id)
        return GraphStateDiff(updated=update_db, created=new_db, deleted=old_db)

    def create_worker(self, database: str, status: Status) -> Value:
        status = Value("h", list(Status).index(status))
        proc = Process(
            target=new_worker,
            kwargs={
                "database": database,
                "status": status,
                "multiprocessing_lock": self.multiprocessing_lock,
            },
        )
        return GraphState(status=status, worker=proc, status_index=status)

    def create_process(self, database: str, status: Status) -> GraphState:
        if database in self.graph_state.keys():
            raise ResourceWarning(f"Listener for {database} already exists")
        new_state = self.create_worker(database=database, status=status)
        self.graph_state[database] = new_state
        new_state.worker.daemon = True
        new_state.worker.start()
        return new_state

    def stop_process(self, database: str):
        if database not in self.graph_state.keys():
            print("Warning: Listener for {} does not exist".format(database))
            return
        worker = self.graph_state[database].worker
        if worker:
            worker.terminate()
            worker.join()
        self.graph_state.pop(database)

    def update_process(self):
        state = self.get_state()
        diff = self.get_state_diff(state)
        print(diff)
        for db_id in diff.created:
            new_graph_state = self.create_process(
                database=db_id, status=state[db_id]
            )
            self.graph_state[db_id] = new_graph_state
        for db_id in diff.updated:
            graph_state = self.graph_state[db_id]
            new_graph_state = GraphState(
                status=state[db_id],
                worker=graph_state.worker,
                status_index=graph_state.status_index,
            )
            self.graph_state[db_id] = new_graph_state
            new_graph_state.status_index.value = list(Status).index(
                state[db_id]
            )
        for db_id in diff.deleted:
            self.stop_process(database=db_id)

        # delete stopped processes
        database_for_delete = []
        for database, state in self.graph_state.items():
            if not state.worker.is_alive():
                database_for_delete.append(database)
        for database in database_for_delete:
            del self.graph_state[database]

    def start(self):
        while True:
            self.tick()
            self.update_process()
