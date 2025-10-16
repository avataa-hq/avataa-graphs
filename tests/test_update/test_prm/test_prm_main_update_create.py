import pytest

from task.models.enums import Status
from updater.updater_parts.helpers.find_node_by_mo_id import find_node_by_mo_id
from updater.updater_parts.updater_abstract import OperationType


@pytest.mark.skip(reason="Not implemented")
def test_prm_create_not_existing(prm_main_updater, prm_data):
    # check before
    node_count_before = prm_main_updater.main_collection.count()
    edge_count_before = prm_main_updater.main_edge_collection.count()
    mo_before = find_node_by_mo_id(
        task=prm_main_updater, mo_id=prm_data.not_existing.mo_id
    )
    prms_count_before = len(mo_before.data.params)

    assert node_count_before > 0
    assert edge_count_before > 0
    assert mo_before is not None
    assert prms_count_before > 0

    # input
    data = [prm_data.not_existing]
    operation = OperationType.CREATED
    status = Status.COMPLETE
    # execute
    prm_main_updater.update_data(status=status, operation=operation, items=data)
    # check after
    node_count_after = prm_main_updater.main_collection.count()
    edge_count_after = prm_main_updater.main_edge_collection.count()
    mo_after = find_node_by_mo_id(
        task=prm_main_updater, mo_id=prm_data.not_existing.mo_id
    )
    prms_count_after = len(mo_after.data.params)

    assert node_count_before == node_count_after
    assert edge_count_before == edge_count_after
    assert prms_count_before == prms_count_after
    assert mo_after == mo_before


@pytest.mark.skip(reason="Not implemented")
def test_prm_create_existing(prm_main_updater, prm_data):
    # check before
    node_count_before = prm_main_updater.main_collection.count()
    edge_count_before = prm_main_updater.main_edge_collection.count()
    mo_before = find_node_by_mo_id(
        task=prm_main_updater, mo_id=prm_data.existing.mo_id
    )
    prms_count_before = len(mo_before.data.params)

    assert node_count_before > 0
    assert edge_count_before > 0
    assert mo_before is not None
    assert prms_count_before > 0

    # input
    data = [prm_data.existing]
    operation = OperationType.CREATED
    status = Status.COMPLETE
    # execute
    prm_main_updater.update_data(status=status, operation=operation, items=data)
    # check after
    node_count_after = prm_main_updater.main_collection.count()
    edge_count_after = prm_main_updater.main_edge_collection.count()
    mo_after = find_node_by_mo_id(
        task=prm_main_updater, mo_id=prm_data.existing.mo_id
    )
    prms_count_after = len(mo_after.data.params)

    assert node_count_before == node_count_after
    assert edge_count_before == edge_count_after
    assert prms_count_before == prms_count_after
    assert mo_after != mo_before


@pytest.mark.skip(reason="Not implemented")
def test_prm_create_existing_mo_link(prm_main_updater, prm_data):
    # check before
    node_count_before = prm_main_updater.main_collection.count()
    edge_count_before = prm_main_updater.main_edge_collection.count()
    mo_before = find_node_by_mo_id(
        task=prm_main_updater, mo_id=prm_data.existing_mo_link.mo_id
    )
    prms_count_before = len(mo_before.data.params)

    assert node_count_before > 0
    assert edge_count_before > 0
    assert mo_before is not None
    assert prms_count_before > 0

    # input
    data = [prm_data.existing_mo_link]
    operation = OperationType.DELETED
    status = Status.COMPLETE
    # execute
    prm_main_updater.update_data(status=status, operation=operation, items=data)
    operation = OperationType.CREATED
    prm_main_updater.update_data(status=status, operation=operation, items=data)
    # check after
    node_count_after = prm_main_updater.main_collection.count()
    edge_count_after = prm_main_updater.main_edge_collection.count()
    mo_after = find_node_by_mo_id(
        task=prm_main_updater, mo_id=prm_data.existing_mo_link.mo_id
    )
    prms_count_after = len(mo_after.data.params)

    assert node_count_before == node_count_after
    assert edge_count_before == edge_count_after
    assert prms_count_before == prms_count_after


@pytest.mark.skip(reason="Not implemented")
def test_prm_create_existing_group(prm_main_updater, prm_data):
    # check before
    node_count_before = prm_main_updater.main_collection.count()
    edge_count_before = prm_main_updater.main_edge_collection.count()
    mo_before = find_node_by_mo_id(
        task=prm_main_updater, mo_id=prm_data.group.mo_id
    )
    prms_count_before = len(mo_before.data.params)

    assert node_count_before > 0
    assert edge_count_before > 0
    assert mo_before is not None
    assert prms_count_before > 0

    # input
    data = [prm_data.group]
    status = Status.COMPLETE
    # execute
    operation = OperationType.DELETED
    prm_main_updater.update_data(status=status, operation=operation, items=data)
    operation = OperationType.CREATED
    prm_main_updater.update_data(status=status, operation=operation, items=data)
    # check after
    node_count_after = prm_main_updater.main_collection.count()
    edge_count_after = prm_main_updater.main_edge_collection.count()
    mo_after = find_node_by_mo_id(
        task=prm_main_updater, mo_id=prm_data.group.mo_id
    )
    prms_count_after = len(mo_after.data.params)

    assert node_count_before == node_count_after
    assert edge_count_before == edge_count_after
    assert prms_count_before == prms_count_after
