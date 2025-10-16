import pytest


@pytest.fixture(scope="function", autouse=True)
def create_default_graph(client):
    req = {"tmo_id": 42588, "name": "first_graph"}
    res = client.post(url="/api/graph/v1/initialisation/", json=req)
    assert res.status_code == 202


@pytest.mark.skip(reason="Not implemented")
def test_graph_initialisation_db_creation(client, get_sys_db):
    """
    GET /api/graph/v1/initialisation

        It has to create graph and create database, based on requested TMO
    """

    databases_list = get_sys_db.databases()
    assert f"tmoId_{42588}" in databases_list


@pytest.mark.skip(reason="Not implemented")
def test_graph_initialisation_collection_creation(
    arango_client, client, get_sys_db
):
    """
    POST /api/graph/v1/initialisation

        It has to create graph and create collection, based on requested TMO
    """

    # check creation collection in system DB
    collections_list = [
        collection_name["name"] for collection_name in get_sys_db.collections()
    ]
    assert "main_graphs" in collections_list

    # check creation collection in DB based on tmo with id 1
    tmo_db = arango_client.db(name="tmoId_42588", username="root", password="")
    tmo_collections = [
        collection_name["name"] for collection_name in tmo_db.collections()
    ]
    assert "tmo" in tmo_collections
    assert "tmoEdge" in tmo_collections
    assert "main" in tmo_collections
    assert "config" in tmo_collections


def test_name_duplicate_error_graph_initialisation(client, get_sys_db):
    """
    POST /api/graph/v1/initialisation

        It has to create graph and create database, but if user want to create graph, which is already exists, we
        need to raise error, because of similar graph name as in DB
    """

    req = {"tmo_id": 2, "name": "first_graph"}
    res = client.post(url="/api/graph/v1/initialisation/", json=req)
    assert res.status_code == 409
    assert res.json() == {
        "detail": "Record with {'name': 'first_graph'} already exists"
    }


def test_tmo_id_duplicate_error_graph_initialisation(client, get_sys_db):
    """
    POST /api/graph/v1/initialisation

        It has to create graph and create database, but if user want to create graph, which is already exists, we
        need to raise error, because of similar graph TMO ID as in DB
    """
    req = {"tmo_id": 42588, "name": "first_graph1"}
    res = client.post(url="/api/graph/v1/initialisation/", json=req)
    assert res.status_code == 409
    assert res.json() == {
        "detail": "Record with {'tmo_id': 42588} already exists"
    }


def test_tmo_doesnt_exists_graph_initialisation(client, get_sys_db):
    """
    POST /api/graph/v1/initialisation

        It has to create graph and create database, but if user want to create graph, by TMO which is not exists
    """
    tmo_id_which_not_exists = 1111
    req = {
        "tmo_id": tmo_id_which_not_exists,  # this tmo doesn't exist
        "name": "first_graph1",
    }
    res = client.post(url="/api/graph/v1/initialisation/", json=req)
    assert res.status_code == 409
    assert res.json() == {
        "detail": f"TMO with id {tmo_id_which_not_exists} does not exist in inventory"
    }


def test_main_graph_record_creation(client, get_sys_db):
    """
    POST /api/graph/v1/initialisation

        When we create some graph we create collection 'main_graphs' in _system DB, so we need to create
        if it creates correctly
    """

    cursor = get_sys_db.aql.execute("FOR doc IN main_graphs RETURN doc")
    data = [document for document in cursor]
    assert len(data) == 1

    data_of_row = lambda key: data[0][key]  # noqa: E731

    assert data_of_row(key="name") == "first_graph"
    assert data_of_row(key="tmo_id") == 42588
    assert data_of_row(key="status") == "New"
    assert data_of_row(key="database") == "tmoId_42588"
    assert data_of_row(key="active_tmo_ids") == [
        42588,
        42622,
        42589,
        42590,
        42604,
        42599,
        42616,
        42600,
        42617,
        42592,
        42611,
        42591,
        42610,
    ]


@pytest.mark.skip(reason="Not implemented")
def test_tmo_record_creation(client, get_sys_db, arango_client):
    """
    POST /api/graph/v1/initialisation

        When we create some graph we create collection 'tmo' in specific tmo DB, so we need to check
        how much objects were created in this collection and what keys were created
    """

    tmo_db = arango_client.db(name="tmoId_42588", username="root", password="")
    cursor = tmo_db.aql.execute("FOR doc IN tmo RETURN doc")
    data = [document for document in cursor]
    assert len(data) == 13
    assert [key["_key"] for key in data] == [
        "42588",
        "42622",
        "42589",
        "42590",
        "42604",
        "42599",
        "42616",
        "42600",
        "42617",
        "42592",
        "42611",
        "42591",
        "42610",
    ]

    main_keys = [
        "description",
        "enabled",
        "geometry_type",
        "global_uniqueness",
        "icon",
        "id",
        "lifecycle_process_definition",
        "materialize",
        "name",
        "p_id",
        "params",
        "point_tmo_const",
        "virtual",
        "_id",
        "_key",
        "_rev",
    ]

    # for test, we can get first object to check quantity and is data correct
    data = data[0]
    assert main_keys.sort() == list(data.keys()).sort()


@pytest.mark.skip(reason="Not implemented")
def test_edge_record_creation(client, get_sys_db, arango_client):
    """
    POST /api/graph/v1/initialisation

        When we create some graph we create collection 'edge' in specific tmo DB, so we need to check
        how much edges were created in this collection.
    """

    tmo_db = arango_client.db(name="tmoId_42588", username="root", password="")
    cursor = tmo_db.aql.execute("FOR doc IN tmoEdge RETURN doc")
    edges = [document for document in cursor]
    assert len(edges) == 23
    assert [[edge["_from"], edge["_to"]] for edge in edges] == [
        ["tmo/42622", "tmo/42588"],
        ["tmo/42589", "tmo/42588"],
        ["tmo/42590", "tmo/42589"],
        ["tmo/42604", "tmo/42590"],
        ["tmo/42599", "tmo/42611"],
        ["tmo/42599", "tmo/42589"],
        ["tmo/42616", "tmo/42611"],
        ["tmo/42616", "tmo/42617"],
        ["tmo/42616", "tmo/42599"],
        ["tmo/42600", "tmo/42589"],
        ["tmo/42617", "tmo/42616"],
        ["tmo/42617", "tmo/42600"],
        ["tmo/42592", "tmo/42589"],
        ["tmo/42611", "tmo/42616"],
        ["tmo/42611", "tmo/42592"],
        ["tmo/42591", "tmo/42589"],
        ["tmo/42610", "tmo/42610"],
        ["tmo/42610", "tmo/42591"],
        ["tmo/42604", "tmo/42622"],
        ["tmo/42610", "tmo/42622"],
        ["tmo/42611", "tmo/42622"],
        ["tmo/42616", "tmo/42622"],
        ["tmo/42617", "tmo/42622"],
    ]


def test_get_all_graphs_existing(client, get_sys_db, arango_client):
    """
    GET /api/graph/v1/initialisation

    """

    res = client.get(url="/api/graph/v1/initialisation/")

    assert len(res.json()) == 1
    res = res.json()[0]

    # because of key creates randomly every time -- we can't predict it
    res.pop("key")

    assert res == {
        "name": "first_graph",
        "tmo_id": 42588,
        "status": "New",
        "error_description": None,
    }


def test_error_update_not_crated_graph_by_key(
    client, get_sys_db, arango_client
):
    """
    PATCH /api/graph/v1/initialisation

        If we try to update graph, which now exists, we can't update it
    """

    res = client.patch(
        url="/api/graph/v1/initialisation/11111", json={"name": "first_graph"}
    )

    assert res.json() == {"detail": "Key 11111 not exist"}


def test_update_graph_name(client, get_sys_db, arango_client):
    """
    PATCH /api/graph/v1/initialisation

        If we try to update graph name, by name of another created graph we need to raise error

    """

    # get key of graph with name 'first_graph'
    graphs = client.get(url="/api/graph/v1/initialisation/")
    graph_key = graphs.json()[0]["key"]

    res = client.patch(
        url=f"/api/graph/v1/initialisation/{graph_key}",
        json={"name": "first_graph1"},
    )
    res = res.json()
    del res["key"]
    assert res == {
        "name": "first_graph1",
        "tmo_id": 42588,
        "status": "New",
        "error_description": None,
    }


def test_error_delete_not_existing_graph(client, get_sys_db, arango_client):
    """
    DELETE /api/graph/v1/initialisation

        If we try to delete graph, by key, when there are no graph with this key

    """

    res = client.delete(url="/api/graph/v1/initialisation/11111")
    assert res.json() == {"detail": "Key 11111 not exist"}


@pytest.mark.skip(reason="Not implemented")
def test_delete_existing_graph(client, get_sys_db, arango_client):
    """
    DELETE /api/graph/v1/initialisation

        If we try to delete graph, by key, when there are no graph with this key

    """

    # check if graph was created
    tmo_db = arango_client.db("tmoId_42588", username="root", password="")
    tmo_data = tmo_db.aql.execute("FOR doc IN tmo RETURN doc")
    data_from_tmo = [i for i in tmo_data]
    assert data_from_tmo

    # get key of graph with name 'first_graph'
    graphs = client.get(url="/api/graph/v1/initialisation/")
    graph_key = graphs.json()[0]["key"]

    # we delete graph
    res = client.delete(url=f"/api/graph/v1/initialisation/{graph_key}")

    # check if tmo with id 42588 database exists
    assert "tmoId_42588" not in get_sys_db.databases()
    assert res.status_code == 204


# def test_error_update_not_crated_graph_by_name(client, get_sys_db, arango_client):
#     """
#         PATCH api/graph/v1/initialisation endpoint
#
#           If we try to update graph name, by name of another created graph we need to raise error
#
#     """
#
#     # create second graph
#     req = {
#         'tmo_id': 1,
#         'name': 'first_graph1'
#     }
#     client.post(url='/api/graph/v1/initialisation/', json=req)
#
#     # get key of first graph with name 'first_graph'
#     graphs = client.get(url='/api/graph/v1/initialisation/')
#     a = graphs.json()
#     graph_key = graphs.json()[0]['key']
#
#     # try to update first graph by name, which exists in second graph
#     res = client.patch(url=f'/api/graph/v1/initialisation/{graph_key}', json={"name": 'first_graph1'})
#     assert res.json() == {'detail': 'first_graph1 is exist'}
