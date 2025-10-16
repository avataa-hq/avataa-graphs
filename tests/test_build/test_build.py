from unittest.mock import Mock

import pytest


@pytest.fixture(scope="function", autouse=True)
def create_default_graph(client):
    req = {"tmo_id": 42588, "name": "first_graph"}
    res = client.post(url="/api/graph/v1/initialisation/", json=req)
    assert res.status_code == 202


@pytest.mark.skip(reason="Not implemented")
def test_error_build_graph_which_doesnt_exists(client):
    """
    POST /api/graph/v1/building

       This endpoint build graph.
       But if graph, on which we want to build doesn't exist -- we need to get error
    """
    res = client.post(url="/api/graph/v1/building/", params={"key": 1111})

    assert res.status_code == 404
    assert res.json() == {"detail": "Document with key 1111 not found"}


@pytest.mark.skip(reason="Not implemented")
def test_build_graph(client):
    """
    POST /api/graph/v1/building

       This endpoint build graph.
       So after building status of graph muct change from "New" to "Complete"
    """
    # get graph key. it generates randomly
    graphs = client.get(url="/api/graph/v1/initialisation/")
    graph_key = graphs.json()[0]["key"]

    # by default, it equals 'New'
    graph_status = graphs.json()[0]["status"]
    assert graph_status == "New"

    # SECOND.build graph
    # because we create Process(task) -- we need mock endpoint, which creates process by instantly
    # run code, which build graph. without creating process
    from services.instances import run_building_in_new_process

    def new_build_graph_in_new_process(key):
        run_building_in_new_process(key=key)

    # it doesn't use, but we change it in memory by Mock
    build_graph_in_new_process = Mock(  # noqa
        side_effect=new_build_graph_in_new_process(graph_key)
    )

    res = client.post(url="/api/graph/v1/building/", params={"key": graph_key})

    assert res.status_code == 202

    # we need check if status of graph after building changed from 'New' to "Complete"
    graphs = client.get(url="/api/graph/v1/initialisation/")

    # after building, it must be changed to 'Complete'
    graph_status = graphs.json()[0]["status"]
    assert graph_status == "Complete"
