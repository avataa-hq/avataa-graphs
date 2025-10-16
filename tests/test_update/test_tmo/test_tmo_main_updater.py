import pytest

from task.models.enums import Status
from tests.test_update.test_tmo.data import (
    existing_tmo,
    main_tmo,
    not_existing_tmo,
)
from updater.updater_parts.updater_abstract import OperationType


@pytest.mark.skip(reason="Not implemented")
def test_delete_main_tmo(tmo_main_updater):
    document = tmo_main_updater.document
    # input
    data = [main_tmo]
    operation = OperationType.DELETED
    status = Status.COMPLETE
    # execute
    with pytest.raises(SystemExit) as execinfo:
        tmo_main_updater.update_data(
            status=status, operation=operation, items=data
        )
        assert execinfo.type is SystemExit
        assert execinfo.value.code == 0
    assert document.database not in tmo_main_updater.sys_db.databases()
    main_collection = tmo_main_updater.sys_db.collection(name="main_graphs")
    assert main_collection.get({"_id": document.id}) is None


@pytest.mark.skip(reason="Not implemented")
def test_delete_not_existing_node(tmo_main_updater):
    # check before
    tmo_ids_before = tmo_main_updater.document.active_tmo_ids
    # input
    data = [not_existing_tmo]
    operation = OperationType.DELETED
    status = Status.COMPLETE
    # execute
    tmo_main_updater.update_data(status=status, operation=operation, items=data)
    # check after
    tmo_ids_after = tmo_main_updater.document.active_tmo_ids
    assert set(tmo_ids_before) == set(tmo_ids_after)


@pytest.mark.skip(reason="Not implemented")
def test_delete_existing_node(tmo_main_updater):
    # check before
    tmo_ids_before = tmo_main_updater.document.active_tmo_ids
    # input
    data = [existing_tmo]
    operation = OperationType.DELETED
    status = Status.COMPLETE
    # execute
    tmo_main_updater.update_data(status=status, operation=operation, items=data)
    # check after
    tmo_ids_after = tmo_main_updater.document.active_tmo_ids
    assert set(tmo_ids_before).difference(set(tmo_ids_after)) == set(
        [i.tmo_id for i in data]
    )


@pytest.mark.skip(reason="Not implemented")
def test_delete_combined_nodes(tmo_main_updater):
    # check before
    tmo_ids_before = tmo_main_updater.document.active_tmo_ids
    # input
    data = [existing_tmo, not_existing_tmo]
    operation = OperationType.DELETED
    status = Status.COMPLETE
    # execute
    tmo_main_updater.update_data(status=status, operation=operation, items=data)
    # check after
    tmo_ids_after = tmo_main_updater.document.active_tmo_ids
    assert set(tmo_ids_before).difference(set(tmo_ids_after)).intersection(
        [i.tmo_id for i in data]
    ) == {existing_tmo.tmo_id}

    # execute
    tmo_ids_before = tmo_main_updater.document.active_tmo_ids
    tmo_main_updater.update_data(status=status, operation=operation, items=data)
    # check after
    tmo_ids_after = tmo_main_updater.document.active_tmo_ids
    assert tmo_ids_before == tmo_ids_after


@pytest.mark.skip(reason="Not implemented")
def test_create_not_existing_node(tmo_main_updater):
    # check before
    tmo_ids_before = tmo_main_updater.document.active_tmo_ids
    # input
    data = [not_existing_tmo]
    operation = OperationType.CREATED
    status = Status.COMPLETE
    # execute
    tmo_main_updater.update_data(status=status, operation=operation, items=data)
    # check after
    tmo_ids_after = tmo_main_updater.document.active_tmo_ids
    assert set(tmo_ids_after).difference(set(tmo_ids_before)) == set(
        [i.tmo_id for i in data]
    )


@pytest.mark.skip(reason="Not implemented")
def test_create_existing_node(tmo_main_updater):
    # check before
    tmo_ids_before = tmo_main_updater.document.active_tmo_ids
    # input
    data = [existing_tmo]
    operation = OperationType.CREATED
    status = Status.COMPLETE
    # execute
    tmo_main_updater.update_data(status=status, operation=operation, items=data)
    # check after
    tmo_ids_after = tmo_main_updater.document.active_tmo_ids
    assert set(tmo_ids_after).difference(set(tmo_ids_before)) == set()
