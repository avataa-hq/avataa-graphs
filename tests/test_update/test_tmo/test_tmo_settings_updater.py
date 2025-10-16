import pytest

from task.models.enums import Status
from tests.test_update.test_tmo.data import (
    group_tmo,
    not_existing_tmo,
    trace_tmo,
)
from updater.updater_parts.updater_abstract import OperationType


@pytest.mark.skip(reason="Not implemented")
def test_delete_not_existing_node(tmo_settings_updater):
    # check before
    group_by_before = tmo_settings_updater.group_by_tprm_ids
    trace_tmo_id_before = tmo_settings_updater.trace_tmo_id
    # input
    data = [not_existing_tmo]
    operation = OperationType.DELETED
    status = Status.COMPLETE
    # execute
    tmo_settings_updater.update_data(
        status=status, operation=operation, items=data
    )
    # check after
    group_by_after = tmo_settings_updater.group_by_tprm_ids
    trace_tmo_id_after = tmo_settings_updater.trace_tmo_id

    assert set(group_by_before) == set(group_by_after)
    assert trace_tmo_id_before == trace_tmo_id_after


@pytest.mark.skip(reason="Not implemented")
def test_delete_trace_tmo(tmo_settings_updater):
    # check before
    group_by_before = tmo_settings_updater.group_by_tprm_ids
    trace_tmo_id_before = tmo_settings_updater.trace_tmo_id
    # input
    data = [trace_tmo]
    operation = OperationType.DELETED
    status = Status.COMPLETE
    # execute
    tmo_settings_updater.update_data(
        status=status, operation=operation, items=data
    )
    # check after
    group_by_after = tmo_settings_updater.group_by_tprm_ids
    trace_tmo_id_after = tmo_settings_updater.trace_tmo_id

    assert set(group_by_before) == set(group_by_after)
    assert trace_tmo_id_before != trace_tmo_id_after


@pytest.mark.skip(reason="Not implemented")
def test_delete_group_tmo(tmo_settings_updater):
    # check before
    group_by_before = tmo_settings_updater.group_by_tprm_ids
    assert group_by_before
    trace_tmo_id_before = tmo_settings_updater.trace_tmo_id
    # input
    data = [group_tmo]
    operation = OperationType.DELETED
    status = Status.COMPLETE
    # execute
    tmo_settings_updater.update_data(
        status=status, operation=operation, items=data
    )
    # check after
    group_by_after = tmo_settings_updater.group_by_tprm_ids
    trace_tmo_id_after = tmo_settings_updater.trace_tmo_id

    assert not group_by_after
    assert trace_tmo_id_before == trace_tmo_id_after
