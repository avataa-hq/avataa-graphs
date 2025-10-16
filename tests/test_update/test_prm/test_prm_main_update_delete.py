import pytest

from task.models.enums import Status
from updater.updater_parts.helpers.find_node_by_mo_id import find_node_by_mo_id
from updater.updater_parts.prm_updater_parts.delete_prm import find_group_node
from updater.updater_parts.updater_abstract import OperationType


@pytest.mark.skip(reason="Not implemented")
def test_prm_delete_not_existing(prm_main_updater, prm_data):
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
    operation = OperationType.DELETED
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
def test_prm_delete_existing(prm_main_updater, prm_data):
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
    operation = OperationType.DELETED
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
    assert prms_count_before == prms_count_after + 1


@pytest.mark.skip(reason="Not implemented")
def test_prm_delete_existing_mo_link(prm_main_updater, prm_data):
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
    # check after
    node_count_after = prm_main_updater.main_collection.count()
    edge_count_after = prm_main_updater.main_edge_collection.count()
    mo_after = find_node_by_mo_id(
        task=prm_main_updater, mo_id=prm_data.existing_mo_link.mo_id
    )
    prms_count_after = len(mo_after.data.params)

    assert node_count_before == node_count_after
    assert edge_count_before > edge_count_after
    assert prms_count_before == prms_count_after + 1
    assert mo_after != mo_before


@pytest.mark.skip(reason="Not implemented")
def test_prm_delete_existing_group(prm_main_updater, prm_data):
    # check before
    node_count_before = prm_main_updater.main_collection.count()
    edge_count_before = prm_main_updater.main_edge_collection.count()
    mo_before = find_node_by_mo_id(
        task=prm_main_updater, mo_id=prm_data.group.mo_id
    )
    prms_count_before = len(mo_before.data.params)
    group_node_before = find_group_node(
        task=prm_main_updater,
        real_mo_node=mo_before,
        tprm_id=prm_data.group.tprm_id,
    )

    assert node_count_before > 0
    assert edge_count_before > 0
    assert mo_before is not None
    assert prms_count_before > 0
    assert group_node_before is not None

    # input
    data = [prm_data.group]
    operation = OperationType.DELETED
    status = Status.COMPLETE
    # execute
    prm_main_updater.update_data(status=status, operation=operation, items=data)
    # check after
    node_count_after = prm_main_updater.main_collection.count()
    edge_count_after = prm_main_updater.main_edge_collection.count()
    mo_after = find_node_by_mo_id(
        task=prm_main_updater, mo_id=prm_data.group.mo_id
    )
    prms_count_after = len(mo_after.data.params)
    group_node_after = find_group_node(
        task=prm_main_updater,
        real_mo_node=mo_after,
        tprm_id=prm_data.group.tprm_id,
    )

    assert node_count_before == node_count_after
    assert edge_count_before == edge_count_after + 1
    assert prms_count_before == prms_count_after + 1
    assert mo_after.breadcrumbs != mo_before.breadcrumbs
    assert group_node_after is None
