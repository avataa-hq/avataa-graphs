import json

import pytest
from testcontainers.arangodb import ArangoDbContainer

from config import GraphDBConfig
from services.graph import GraphService
from tests.test_update.initiation_data.init import initialize_data
from tests.test_update.mocks import InventoryMock
from tests.test_update.test_mo.data import TestMoData
from tests.test_update.test_prm.data import TestPrmData
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


@pytest.fixture(scope="session")
def arango_container_url():
    with ArangoDbContainer(arango_no_auth=True) as arango:
        yield arango.get_connection_url()


@pytest.fixture(scope="session")
def graph_service(arango_container_url):
    client = GraphService(url=arango_container_url, username="", password="")
    return client


@pytest.fixture(scope="function", autouse=True)
def init_data(graph_service, cwd):
    config = GraphDBConfig()
    initialize_data(graph_service=graph_service, config=config, cwd=cwd)


@pytest.fixture(scope="session")
def mock_inventory():
    inventory_mock = InventoryMock()
    return inventory_mock


@pytest.fixture(scope="session")
def tmo_main_updater(graph_service):
    updater = TmoMainUpdater(graph_db=graph_service, key="26281740")
    return updater


@pytest.fixture(scope="session")
def tmo_tmo_updater(graph_service, mock_inventory):
    updater = TmoTmoUpdater(
        graph_db=graph_service, key="26281740", inventory=mock_inventory
    )
    return updater


@pytest.fixture(scope="session")
def tmo_settings_updater(graph_service):
    updater = TmoSettingsUpdater(graph_db=graph_service, key="26281740")
    return updater


@pytest.fixture(scope="session")
def tprm_settings_updater(graph_service) -> TprmSettingUpdater:
    updater = TprmSettingUpdater(graph_db=graph_service, key="26281740")
    return updater


@pytest.fixture(scope="session")
def tprm_tmo_updater(graph_service, mock_inventory) -> TprmTmoUpdater:
    updater = TprmTmoUpdater(
        graph_db=graph_service, key="26281740", inventory=mock_inventory
    )
    return updater


@pytest.fixture(scope="session")
def mo_main_updater(graph_service, mock_inventory) -> MoGraphUpdater:
    updater = MoGraphUpdater(
        graph_db=graph_service, key="26281740", inventory=mock_inventory
    )
    return updater


@pytest.fixture(scope="session")
def prm_main_updater(graph_service, mock_inventory) -> PrmGraphUpdater:
    updater = PrmGraphUpdater(
        graph_db=graph_service, key="26281740", inventory=mock_inventory
    )
    return updater


@pytest.fixture(scope="session")
def mo_data(cwd) -> TestMoData:
    with open(
        f"{cwd}/tests/test_update/initiation_data/data/databases/tmoId_43623/main.json"
    ) as f:
        mo_db_data = json.load(f)
    data = TestMoData(data=mo_db_data)
    return data


@pytest.fixture(scope="session")
def prm_data(cwd) -> TestPrmData:
    with open(
        f"{cwd}/tests/test_update/initiation_data/data/databases/tmoId_43623/main.json"
    ) as f:
        prm_db_data = json.load(f)
    data = TestPrmData(data=prm_db_data)
    return data
