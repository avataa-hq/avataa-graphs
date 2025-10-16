from task.models.incoming_data import TMO

not_existing_tmo = TMO(
    p_id=None,
    name="Test",
    virtual=False,
    global_uniqueness=True,
    materialize=False,
    point_tmo_const=[1, 2, 3],
    id=1,
)
existing_tmo = TMO(
    p_id=43623,
    name="TN Cable",
    virtual=False,
    global_uniqueness=True,
    materialize=False,
    point_tmo_const=[43633],
    id=44483,
)
main_tmo = TMO(
    p_id=43621,
    name="TN Regions",
    virtual=False,
    global_uniqueness=True,
    materialize=False,
    point_tmo_const=[],
    id=43623,
)

trace_tmo = TMO(
    p_id=43622,
    name="TN Service",
    virtual=False,
    global_uniqueness=True,
    materialize=False,
    point_tmo_const=[],
    id=43626,
)

group_tmo = TMO(
    p_id=43623,
    name="TN Locations",
    virtual=False,
    global_uniqueness=True,
    materialize=False,
    point_tmo_const=[43622],
    id=43622,
)

create_tmo = TMO(
    p_id=43622,
    name="Creating Test",
    virtual=False,
    global_uniqueness=True,
    materialize=False,
    point_tmo_const=[],
    id=100500,
)
