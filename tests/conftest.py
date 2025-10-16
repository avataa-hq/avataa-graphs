import os
import pathlib
import sys

sys.path.append(os.path.join(sys.path[0], "app"))
from unittest.mock import patch

from arango import ArangoClient
from fastapi.testclient import TestClient
import pytest
from testcontainers.arangodb import ArangoDbContainer

from config import ArangoConfig, SecurityConfig
from services.inventory import Inventory as MainInventory
from tests.test_config import (
    ARANGO_URL,
    ArangoTestConfig,
    arango_db_settings,
    arango_test_config,
)
from tests.test_config import (
    Inventory as MockInventory,
)

__arango_url = ""


# Create arango container before Test
if arango_test_config.run_container_arango_local:

    class ArangoDockerDBContainer(ArangoDbContainer):
        @property
        def connection_url(self, host: str | None = None) -> str:
            if not host:
                host = os.environ.get("TEST_DOCKER_DB_HOST")
            return str(super().get_connection_url(host=host))

    @pytest.fixture(scope="session", autouse=True)
    def arango_client():
        with ArangoDockerDBContainer(ARANGO_URL, arango_no_auth=True) as arango:
            client = ArangoClient(hosts=arango.get_connection_url())
            global __arango_url
            __arango_url = arango.get_connection_url()
            yield client

# Use exist container
else:

    @pytest.fixture(scope="session", autouse=True)
    def arango_client():
        client = ArangoClient(hosts=arango_test_config.url)
        global __arango_url
        __arango_url = arango_test_config.url
        yield client


@pytest.fixture(scope="session", autouse=True)
def get_sys_db(arango_client):
    sys_db = arango_client.db(name="_system", **arango_db_settings())
    return sys_db


def get_url():
    return __arango_url


@pytest.fixture(scope="function", autouse=True)
def client(mocker):
    mocker.patch(
        "config.ArangoConfig",
        side_effect=ArangoTestConfig,
    )
    mocker.patch(
        "config.SecurityConfig",
        return_value=SecurityConfig(security_type="DISABLE"),
    )
    from app.main import app

    client = TestClient(app)
    yield client


@pytest.fixture(scope="session", autouse=True)
def main_mocker():
    mock_inventory_grpc = MockInventory(grpc_url="")

    class MockArango:
        def __call__(self, *args, **kwargs):
            self.url = get_url()
            self.username = "root"
            self.password = " "  # noqa: S105
            self.host = "localhost"
            self.protocol = "https"

            return self

    with (
        # ARANGO CONFIG
        patch.object(ArangoConfig, "__new__", new_callable=MockArango),
        # INVENTORY GRPC
        patch.object(
            MainInventory, "get_tmo_tree", new=mock_inventory_grpc.get_tmo_tree
        ),
        patch.object(
            MainInventory,
            "get_tprms_by_tmo_id",
            new=mock_inventory_grpc.get_tprms_by_tmo_id,
        ),
        patch.object(
            MainInventory,
            "get_mos_by_tmo_id",
            new=mock_inventory_grpc.get_mos_by_tmo_id,
        ),
        patch.object(
            MainInventory,
            "_convert_prm_val_type",
            new=mock_inventory_grpc._convert_prm_val_type,
        ),
        patch.object(
            MainInventory, "_convert_mo", new=mock_inventory_grpc._convert_mo
        ),
        patch.object(
            MainInventory,
            "get_tmo_by_mo_id",
            new=mock_inventory_grpc.get_tmo_by_mo_id,
        ),
        patch.object(
            MainInventory,
            "get_mos_by_mo_ids",
            new=mock_inventory_grpc.get_mos_by_mo_ids,
        ),
        patch.object(
            MainInventory,
            "get_prms_by_prm_ids",
            new=mock_inventory_grpc.get_prms_by_prm_ids,
        ),
    ):
        yield


@pytest.fixture(scope="function", autouse=True)
def databases_cleaner(get_sys_db):
    database_list = get_sys_db.databases()
    database_list.remove("_system")

    for _database in database_list:
        get_sys_db.delete_database(_database)


@pytest.fixture(scope="function", autouse=True)
def collections_cleaner(get_sys_db):
    get_sys_db.delete_collection("main_graphs", ignore_missing=True)


CWD = pathlib.Path(__file__).resolve().parents[1]


@pytest.fixture(scope="session")
def cwd():
    return CWD
