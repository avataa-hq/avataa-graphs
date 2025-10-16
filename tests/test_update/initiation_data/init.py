import json
import os.path

from config import GraphDBConfig
from services.graph import GraphService
from tests.test_update.initiation_data.fill_databases import fill_databases
from tests.test_update.initiation_data.init_databases import init_databases
from tests.test_update.initiation_data.init_main_record import init_main_records


def get_main_records(cwd: str):
    file_url = os.path.join(
        cwd, f"{cwd}/tests/test_update/initiation_data/data/main_records.json"
    )
    with open(file_url, "r") as f:
        main_records = json.load(f)
    return main_records


def initialize_data(
    graph_service: GraphService, config: GraphDBConfig, cwd: str = "."
):
    data = get_main_records(cwd)
    init_main_records(graph_service=graph_service, config=config, data=data)
    init_databases(graph_service=graph_service, config=config, data=data)
    fill_databases(graph_service=graph_service, data=data, cwd=cwd)
