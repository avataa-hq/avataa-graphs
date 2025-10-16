from multiprocessing import Lock, Process
import time

from config import ArangoConfig, GraphDBConfig, InventoryGRPCConfig
from services.graph import GraphService
from services.inventory import Inventory
from task.building_tasks import RunBuildingTask
from task.on_start import OnStartTask

inventory = Inventory(InventoryGRPCConfig().url)
graph_db = GraphService(
    url=ArangoConfig().url,
    username=ArangoConfig().username,
    password=ArangoConfig().password,
    sys_database_name=GraphDBConfig().sys_database_name,
)

# Checking unfinished processes when starting the application
OnStartTask(graphdb=graph_db).execute()


def run_building_in_new_process(key: str, lock: Lock):
    instance_graphdb = graph_db
    # instance_inventory = inventory
    instance_inventory = Inventory(InventoryGRPCConfig().url, lock=lock)
    instance = RunBuildingTask(
        graph_db=instance_graphdb, inventory=instance_inventory, key=key
    )
    instance.execute()


def build_graph_in_new_process(key: str, daemon: bool = True):
    lock = Lock()
    task = Process(
        target=run_building_in_new_process,
        kwargs={"key": key, "lock": lock},
        daemon=daemon,
    )
    start_time = time.perf_counter()
    task.start()
    task.join()
    end_time = time.perf_counter()
    print(f"Elapsed time for build graph: {end_time - start_time:.2f}")


def create_db_connection_instance():
    graph_db_instance = GraphService(
        url=ArangoConfig().url,
        username=ArangoConfig().username,
        password=ArangoConfig().password,
        sys_database_name=GraphDBConfig().sys_database_name,
    )
    return graph_db_instance


graph_db = create_db_connection_instance()
