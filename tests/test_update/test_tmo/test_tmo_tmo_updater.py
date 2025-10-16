import pytest

from task.models.enums import Status
from tests.test_update.test_tmo.data import (
    create_tmo,
    existing_tmo,
    not_existing_tmo,
)
from updater.updater_parts.updater_abstract import OperationType


@pytest.mark.skip(reason="Not implemented")
def test_delete_not_existing_node(tmo_tmo_updater):
    # check before
    node_length_before = tmo_tmo_updater.tmo_collection.count()
    edge_length_before = tmo_tmo_updater.tmo_edge_collection.count()
    assert node_length_before > 0
    assert edge_length_before > 0
    # input
    data = [not_existing_tmo]
    operation = OperationType.DELETED
    status = Status.COMPLETE
    # execute
    tmo_tmo_updater.update_data(status=status, operation=operation, items=data)
    # check after
    node_length_after = tmo_tmo_updater.tmo_collection.count()
    edge_length_after = tmo_tmo_updater.tmo_edge_collection.count()
    assert node_length_before == node_length_after
    assert edge_length_before == edge_length_after


@pytest.mark.skip(reason="Not implemented")
def test_delete_existing_node(tmo_tmo_updater):
    # check before
    node_length_before = tmo_tmo_updater.tmo_collection.count()
    edge_length_before = tmo_tmo_updater.tmo_edge_collection.count()
    assert node_length_before > 0
    # input
    data = [existing_tmo]
    operation = OperationType.DELETED
    status = Status.COMPLETE
    # execute
    tmo_tmo_updater.update_data(status=status, operation=operation, items=data)
    # check after
    node_length_after = tmo_tmo_updater.tmo_collection.count()
    edge_length_after = tmo_tmo_updater.tmo_edge_collection.count()
    assert node_length_before - node_length_after == 1
    assert edge_length_before > edge_length_after


@pytest.mark.skip(reason="Not implemented")
def test_delete_combined_nodes(tmo_tmo_updater):
    # check before
    node_length_before = tmo_tmo_updater.tmo_collection.count()
    edge_length_before = tmo_tmo_updater.tmo_edge_collection.count()
    assert node_length_before > 0
    # input
    data = [not_existing_tmo, existing_tmo]
    operation = OperationType.DELETED
    status = Status.COMPLETE
    # execute
    tmo_tmo_updater.update_data(status=status, operation=operation, items=data)
    # check after
    node_length_after = tmo_tmo_updater.tmo_collection.count()
    edge_length_after = tmo_tmo_updater.tmo_edge_collection.count()
    assert node_length_before - node_length_after == 1
    assert edge_length_before > edge_length_after

    # execute
    node_length_before = node_length_after
    edge_length_before = edge_length_after
    tmo_tmo_updater.update_data(status=status, operation=operation, items=data)
    # check after
    node_length_after = tmo_tmo_updater.tmo_collection.count()
    edge_length_after = tmo_tmo_updater.tmo_edge_collection.count()
    assert node_length_before == node_length_after
    assert edge_length_before == edge_length_after


@pytest.mark.skip(reason="Not implemented")
def test_create_node(tmo_tmo_updater):
    # check before
    node_length_before = tmo_tmo_updater.tmo_collection.count()
    edge_length_before = tmo_tmo_updater.tmo_edge_collection.count()
    assert node_length_before > 0
    # input
    data = [create_tmo]
    operation = OperationType.CREATED
    status = Status.COMPLETE
    # execute
    tmo_tmo_updater.update_data(status=status, operation=operation, items=data)
    # check after
    node_length_after = tmo_tmo_updater.tmo_collection.count()
    edge_length_after = tmo_tmo_updater.tmo_edge_collection.count()
    assert node_length_before + 1 == node_length_after
    assert edge_length_before < edge_length_after


@pytest.mark.skip(reason="Not implemented")
def test_update_as_delete(tmo_tmo_updater):
    # check before
    node_length_before = tmo_tmo_updater.tmo_collection.count()
    edge_length_before = tmo_tmo_updater.tmo_edge_collection.count()
    assert node_length_before > 0
    # input
    tmo = existing_tmo.model_copy(deep=True)
    tmo.p_id = 100500
    data = [tmo]
    operation = OperationType.UPDATED
    status = Status.COMPLETE
    # execute
    tmo_tmo_updater.update_data(status=status, operation=operation, items=data)
    # check after
    node_length_after = tmo_tmo_updater.tmo_collection.count()
    edge_length_after = tmo_tmo_updater.tmo_edge_collection.count()

    assert node_length_before == node_length_after + 1
    assert edge_length_before > edge_length_after


@pytest.mark.skip(reason="Not implemented")
def test_update_as_create(tmo_tmo_updater):
    # check before
    node_length_before = tmo_tmo_updater.tmo_collection.count()
    edge_length_before = tmo_tmo_updater.tmo_edge_collection.count()
    assert node_length_before > 0
    # input
    tmo = not_existing_tmo.model_copy(deep=True)
    tmo.p_id = 43623
    data = [tmo]
    operation = OperationType.UPDATED
    status = Status.COMPLETE
    # execute
    tmo_tmo_updater.update_data(status=status, operation=operation, items=data)
    # check after
    node_length_after = tmo_tmo_updater.tmo_collection.count()
    edge_length_after = tmo_tmo_updater.tmo_edge_collection.count()

    assert node_length_before + 2 == node_length_after
    assert edge_length_before < edge_length_after


@pytest.mark.skip(reason="Not implemented")
def test_update_as_update(tmo_tmo_updater):
    # check before
    node_length_before = tmo_tmo_updater.tmo_collection.count()
    edge_length_before = tmo_tmo_updater.tmo_edge_collection.count()
    assert node_length_before > 0
    # input
    tmo = existing_tmo.model_copy(deep=True)
    tmo.p_id = 44481
    data = [tmo]
    operation = OperationType.UPDATED
    status = Status.COMPLETE
    # execute
    tmo_tmo_updater.update_data(status=status, operation=operation, items=data)
    # check after
    node_length_after = tmo_tmo_updater.tmo_collection.count()
    edge_length_after = tmo_tmo_updater.tmo_edge_collection.count()

    assert node_length_before == node_length_after
    assert edge_length_before == edge_length_after
