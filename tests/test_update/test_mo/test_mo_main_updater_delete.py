import pytest

from task.models.enums import Status
from updater.updater_parts.updater_abstract import OperationType


@pytest.mark.skip(reason="Not implemented")
def test_mo_delete_not_existing(mo_main_updater, mo_data):
    # check before
    mo_nodes_count_before = mo_main_updater.main_collection.count()
    mo_edges_count_before = mo_main_updater.main_edge_collection.count()
    mo_path_edges_count_before = mo_main_updater.main_path_collection.count()

    assert mo_nodes_count_before > 0
    assert mo_edges_count_before > 0
    assert mo_path_edges_count_before > 0

    # input
    data = [mo_data.not_existing]
    operation = OperationType.DELETED
    status = Status.COMPLETE

    # execute
    mo_main_updater.update_data(status=status, operation=operation, items=data)

    # check after
    mo_nodes_count_after = mo_main_updater.main_collection.count()
    mo_edges_count_after = mo_main_updater.main_edge_collection.count()
    mo_path_edges_count_after = mo_main_updater.main_path_collection.count()

    assert mo_nodes_count_before == mo_nodes_count_after
    assert mo_edges_count_before == mo_edges_count_after
    assert mo_path_edges_count_before == mo_path_edges_count_after


@pytest.mark.skip(reason="Not implemented")
def test_mo_delete_highest(mo_main_updater, mo_data):
    # input
    data = [mo_data._top_level_node]
    operation = OperationType.DELETED
    status = Status.COMPLETE

    # check
    count_edges_query = """
        LET nodeId = FIRST(
            FOR node IN @@mainCollection
                FILTER NOT_NULL(node.data)
                FILTER node.data.id == @moId
                LIMIT 1
                RETURN node._id
        )
        FOR edge IN @@edgeCollection
            FILTER edge._from == nodeId OR edge._to == nodeId
            COLLECT WITH COUNT INTO length
            RETURN length
    """
    mo_edges_count_binds = {
        "@edgeCollection": mo_main_updater.main_edge_collection.name,
        "@mainCollection": mo_main_updater.main_collection.name,
        "moId": data[0].id,
    }
    mo_path_edges_count_binds = {
        "@edgeCollection": mo_main_updater.main_path_collection.name,
        "@mainCollection": mo_main_updater.main_collection.name,
        "moId": data[0].id,
    }
    # check before
    mo_nodes_count_before = mo_main_updater.main_collection.count()
    mo_edges_count_before = next(
        mo_main_updater.database.aql.execute(
            query=count_edges_query, bind_vars=mo_edges_count_binds
        )
    )
    mo_path_edges_count_before = next(
        mo_main_updater.database.aql.execute(
            query=count_edges_query, bind_vars=mo_path_edges_count_binds
        )
    )

    assert mo_nodes_count_before > 0
    assert mo_edges_count_before > 0
    assert mo_path_edges_count_before > 0

    # execute
    mo_main_updater.update_data(status=status, operation=operation, items=data)

    # check after
    mo_nodes_count_after = mo_main_updater.main_collection.count()
    mo_edges_count_after = next(
        mo_main_updater.database.aql.execute(
            query=count_edges_query, bind_vars=mo_edges_count_binds
        )
    )
    mo_path_edges_count_after = next(
        mo_main_updater.database.aql.execute(
            query=count_edges_query, bind_vars=mo_path_edges_count_binds
        )
    )
    assert mo_nodes_count_before == mo_nodes_count_after + 1
    assert mo_edges_count_before > mo_edges_count_after
    assert mo_edges_count_after == 0
    assert mo_path_edges_count_before > mo_path_edges_count_after
    assert mo_path_edges_count_after == 0
