import pytest

from task.models.dto import DbTmoNode
from task.models.enums import Status
from task.task_abstract import TaskAbstract
from tests.test_update.test_tprm.data import (
    group_tprm,
    not_existing_tprm,
    trace_tprm,
)
from updater.updater_parts.updater_abstract import OperationType


def count_outgoing_links(tmo_node_id: str, task: TaskAbstract) -> int:
    query = """
        FOR edge IN @@tmoEdgeCollection
            FILTER edge._from == @tmoNodeId
            COLLECT WITH COUNT INTO length
            RETURN length
    """
    binds = {
        "@tmoEdgeCollection": task.tmo_edge_collection.name,
        "tmoNodeId": tmo_node_id,
    }
    response = list(task.database.aql.execute(query=query, bind_vars=binds))
    return response[0]


@pytest.mark.skip(reason="Not implemented")
def test_delete_not_existing_tprm(tprm_settings_updater):
    # check before
    group_by_before = tprm_settings_updater.group_by_tprm_ids
    trace_tprm_id_before = tprm_settings_updater.trace_tprm_id
    start_tprm_before = tprm_settings_updater.start_from_tprm
    # input
    data = [not_existing_tprm]
    operation = OperationType.DELETED
    status = Status.COMPLETE
    # execute
    tprm_settings_updater.update_data(
        status=status, operation=operation, items=data
    )
    # check after
    group_by_after = tprm_settings_updater.group_by_tprm_ids
    trace_tprm_id_after = tprm_settings_updater.trace_tprm_id
    start_tprm_after = tprm_settings_updater.start_from_tprm
    assert group_by_before == group_by_after
    assert trace_tprm_id_before == trace_tprm_id_after
    assert start_tprm_before == start_tprm_after


@pytest.mark.skip(reason="Not implemented")
def test_delete_existing_tprm(tprm_settings_updater):
    # check before
    group_by_before = tprm_settings_updater.group_by_tprm_ids
    trace_tprm_id_before = tprm_settings_updater.trace_tprm_id
    start_tprm_before = tprm_settings_updater.start_from_tprm
    assert start_tprm_before is not None
    # input
    data = [group_tprm]
    operation = OperationType.DELETED
    status = Status.COMPLETE
    # execute
    tprm_settings_updater.update_data(
        status=status, operation=operation, items=data
    )
    # check after
    group_by_after = tprm_settings_updater.group_by_tprm_ids
    trace_tprm_id_after = tprm_settings_updater.trace_tprm_id
    start_tprm_after = tprm_settings_updater.start_from_tprm
    assert set(group_by_before).difference({group_tprm.id}) == set(
        group_by_after
    )
    assert trace_tprm_id_before == trace_tprm_id_after
    assert start_tprm_before != start_tprm_after


@pytest.mark.skip(reason="Not implemented")
def test_delete_trace_tprm(tprm_settings_updater):
    # check before
    group_by_before = tprm_settings_updater.group_by_tprm_ids
    trace_tprm_id_before = tprm_settings_updater.trace_tprm_id
    start_tprm_before = tprm_settings_updater.start_from_tprm
    assert trace_tprm_id_before is not None
    # input
    data = [trace_tprm]
    operation = OperationType.DELETED
    status = Status.COMPLETE
    # execute
    tprm_settings_updater.update_data(
        status=status, operation=operation, items=data
    )
    # check after
    group_by_after = tprm_settings_updater.group_by_tprm_ids
    trace_tprm_id_after = tprm_settings_updater.trace_tprm_id
    start_tprm_after = tprm_settings_updater.start_from_tprm
    assert group_by_before == group_by_after
    assert trace_tprm_id_before != trace_tprm_id_after
    assert start_tprm_before == start_tprm_after


@pytest.mark.skip(reason="Not implemented")
def test_tmo_delete_not_existing_tprm(tprm_tmo_updater):
    # check before
    tmo_node = DbTmoNode.model_validate(
        tprm_tmo_updater.tmo_collection.get(
            {"_key": str(not_existing_tprm.tmo_id)}
        )
    )
    tprms_count_before = len(tmo_node.params)
    edge_count_before = count_outgoing_links(
        tmo_node_id=tmo_node.id, task=tprm_tmo_updater
    )
    assert edge_count_before > 0
    assert tprms_count_before > 0
    # input
    data = [not_existing_tprm]
    operation = OperationType.DELETED
    status = Status.COMPLETE
    # execute
    tprm_tmo_updater.update_data(status=status, operation=operation, items=data)
    # check after
    tmo_node = DbTmoNode.model_validate(
        tprm_tmo_updater.tmo_collection.get(
            {"_key": str(not_existing_tprm.tmo_id)}
        )
    )
    tprms_count_after = len(tmo_node.params)
    edge_count_after = count_outgoing_links(
        tmo_node_id=tmo_node.id, task=tprm_tmo_updater
    )
    assert tprms_count_before == tprms_count_after
    assert edge_count_before == edge_count_after


@pytest.mark.skip(reason="Not implemented")
def test_tmo_delete_existing_tprm(tprm_tmo_updater):
    # check before
    tmo_node = DbTmoNode.model_validate(
        tprm_tmo_updater.tmo_collection.get({"_key": str(group_tprm.tmo_id)})
    )
    tprms_count_before = len(tmo_node.params)
    edge_count_before = count_outgoing_links(
        tmo_node_id=tmo_node.id, task=tprm_tmo_updater
    )
    assert edge_count_before > 0
    assert tprms_count_before > 0
    # input
    data = [group_tprm]
    operation = OperationType.DELETED
    status = Status.COMPLETE
    # execute
    tprm_tmo_updater.update_data(status=status, operation=operation, items=data)
    # check after
    tmo_node = DbTmoNode.model_validate(
        tprm_tmo_updater.tmo_collection.get({"_key": str(group_tprm.tmo_id)})
    )
    tprms_count_after = len(tmo_node.params)
    edge_count_after = count_outgoing_links(
        tmo_node_id=tmo_node.id, task=tprm_tmo_updater
    )
    assert tprms_count_before == tprms_count_after + 1
    assert edge_count_before == edge_count_after + 1


@pytest.mark.skip(reason="Not implemented")
def test_tmo_delete_trace_tprm(tprm_tmo_updater):
    # check before
    tmo_node = DbTmoNode.model_validate(
        tprm_tmo_updater.tmo_collection.get({"_key": str(trace_tprm.tmo_id)})
    )
    tprms_count_before = len(tmo_node.params)
    edge_count_before = count_outgoing_links(
        tmo_node_id=tmo_node.id, task=tprm_tmo_updater
    )
    assert edge_count_before > 0
    assert tprms_count_before > 0
    # input
    data = [trace_tprm]
    operation = OperationType.DELETED
    status = Status.COMPLETE
    # execute
    tprm_tmo_updater.update_data(status=status, operation=operation, items=data)
    # check after
    tmo_node = DbTmoNode.model_validate(
        tprm_tmo_updater.tmo_collection.get({"_key": str(trace_tprm.tmo_id)})
    )
    tprms_count_after = len(tmo_node.params)
    edge_count_after = count_outgoing_links(
        tmo_node_id=tmo_node.id, task=tprm_tmo_updater
    )
    assert tprms_count_before == tprms_count_after + 1
    assert edge_count_before == edge_count_after


@pytest.mark.skip(reason="Not implemented")
def test_tmo_create_not_existing_tprm(tprm_tmo_updater):
    # check before
    tmo_node = DbTmoNode.model_validate(
        tprm_tmo_updater.tmo_collection.get(
            {"_key": str(not_existing_tprm.tmo_id)}
        )
    )
    tprms_count_before = len(tmo_node.params)
    edge_count_before = count_outgoing_links(
        tmo_node_id=tmo_node.id, task=tprm_tmo_updater
    )
    assert edge_count_before > 0
    assert tprms_count_before > 0
    # input
    data = [not_existing_tprm]
    operation = OperationType.CREATED
    status = Status.COMPLETE
    # execute
    tprm_tmo_updater.update_data(status=status, operation=operation, items=data)
    # check after
    tmo_node = DbTmoNode.model_validate(
        tprm_tmo_updater.tmo_collection.get(
            {"_key": str(not_existing_tprm.tmo_id)}
        )
    )
    tprms_count_after = len(tmo_node.params)
    edge_count_after = count_outgoing_links(
        tmo_node_id=tmo_node.id, task=tprm_tmo_updater
    )
    assert tprms_count_before + 1 == tprms_count_after
    assert edge_count_before + 1 == edge_count_after


@pytest.mark.skip(reason="Not implemented")
def test_tmo_create_existing_tprm(tprm_tmo_updater):
    # check before
    tmo_node = DbTmoNode.model_validate(
        tprm_tmo_updater.tmo_collection.get({"_key": str(group_tprm.tmo_id)})
    )
    tprms_count_before = len(tmo_node.params)
    edge_count_before = count_outgoing_links(
        tmo_node_id=tmo_node.id, task=tprm_tmo_updater
    )
    assert edge_count_before > 0
    assert tprms_count_before > 0
    # input
    data = [group_tprm]
    operation = OperationType.CREATED
    status = Status.COMPLETE
    # execute
    tprm_tmo_updater.update_data(status=status, operation=operation, items=data)
    # check after
    tmo_node = DbTmoNode.model_validate(
        tprm_tmo_updater.tmo_collection.get({"_key": str(group_tprm.tmo_id)})
    )
    tprms_count_after = len(tmo_node.params)
    edge_count_after = count_outgoing_links(
        tmo_node_id=tmo_node.id, task=tprm_tmo_updater
    )
    assert tprms_count_before == tprms_count_after
    assert edge_count_before == edge_count_after


@pytest.mark.skip(reason="Not implemented")
def test_tmo_update_not_existing_tprm(tprm_tmo_updater):
    # check before
    tmo_node = DbTmoNode.model_validate(
        tprm_tmo_updater.tmo_collection.get(
            {"_key": str(not_existing_tprm.tmo_id)}
        )
    )
    tprms_count_before = len(tmo_node.params)
    edge_count_before = count_outgoing_links(
        tmo_node_id=tmo_node.id, task=tprm_tmo_updater
    )
    assert edge_count_before > 0
    assert tprms_count_before > 0
    # input
    data = [not_existing_tprm]
    operation = OperationType.UPDATED
    status = Status.COMPLETE
    # execute
    tprm_tmo_updater.update_data(status=status, operation=operation, items=data)
    # check after
    tmo_node = DbTmoNode.model_validate(
        tprm_tmo_updater.tmo_collection.get(
            {"_key": str(not_existing_tprm.tmo_id)}
        )
    )
    tprms_count_after = len(tmo_node.params)
    edge_count_after = count_outgoing_links(
        tmo_node_id=tmo_node.id, task=tprm_tmo_updater
    )
    assert tprms_count_before + 1 == tprms_count_after
    assert edge_count_before + 1 == edge_count_after


@pytest.mark.skip(reason="Not implemented")
def test_tmo_update_existing_tprm(tprm_tmo_updater):
    # check before
    tmo_node = DbTmoNode.model_validate(
        tprm_tmo_updater.tmo_collection.get({"_key": str(group_tprm.tmo_id)})
    )
    tprms_count_before = len(tmo_node.params)
    edge_count_before = count_outgoing_links(
        tmo_node_id=tmo_node.id, task=tprm_tmo_updater
    )
    assert edge_count_before > 0
    assert tprms_count_before > 0
    # input
    data = [group_tprm]
    operation = OperationType.CREATED
    status = Status.COMPLETE
    # execute
    tprm_tmo_updater.update_data(status=status, operation=operation, items=data)
    # check after
    tmo_node = DbTmoNode.model_validate(
        tprm_tmo_updater.tmo_collection.get({"_key": str(group_tprm.tmo_id)})
    )
    tprms_count_after = len(tmo_node.params)
    edge_count_after = count_outgoing_links(
        tmo_node_id=tmo_node.id, task=tprm_tmo_updater
    )
    assert tprms_count_before == tprms_count_after
    assert edge_count_before == edge_count_after
