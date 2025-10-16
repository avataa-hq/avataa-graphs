from arango.database import StandardDatabase

from config import GraphDBConfig
from services.graph import GraphService, IfNotExistType
from task.models.dto import DbMainRecord


def init_collections(
    graph_service: GraphService, db: StandardDatabase, config: GraphDBConfig
):
    # tmo
    tmo_collection = graph_service.get_collection(
        db=db,
        name=config.tmo_collection_name,
        if_not_exist=IfNotExistType.CREATE,
    )
    tmo_collection.add_hash_index(fields=["name"], unique=True, sparse=True)

    graph_service.get_collection(
        db=db,
        name=config.tmo_edge_name,
        if_not_exist=IfNotExistType.CREATE,
        edge=True,
    )

    graph_service.create_graph(
        db=db,
        name=config.tmo_graph_name,
        edge_collection=config.tmo_edge_name,
        from_vertex_collections=[config.tmo_collection_name],
        to_vertex_collections=[config.tmo_collection_name],
    )

    graph_service.get_collection(
        db=db,
        name=config.config_collection_name,
        if_not_exist=IfNotExistType.CREATE,
    )

    # mo
    mo_collection = graph_service.get_collection(
        db=db,
        name=config.graph_data_collection_name,
        if_not_exist=IfNotExistType.CREATE,
    )
    mo_collection.add_hash_index(
        fields=["grouped_by_tprm"], unique=False, sparse=True
    )
    mo_collection.add_hash_index(fields=["name"], unique=False, sparse=True)
    mo_collection.add_hash_index(fields=["tmo"], unique=False, sparse=True)

    edge_mo_collection = graph_service.get_collection(
        db=db,
        name=config.graph_data_edge_name,
        if_not_exist=IfNotExistType.CREATE,
        edge=True,
    )
    edge_mo_collection.add_hash_index(
        fields=["connection_type"], unique=False, sparse=True
    )
    edge_mo_collection.add_hash_index(
        fields=["virtual"], unique=False, sparse=True
    )

    graph_service.get_collection(
        db=db,
        name=config.graph_data_graph_name,
        if_not_exist=IfNotExistType.CREATE,
    )

    # search
    db.create_analyzer(
        name="norm_en",
        analyzer_type="norm",
        properties={
            "locale": "en",
            "accent": False,
            "case": "lower",
        },
    )
    mo_collection.add_inverted_index(
        fields=[  # type: ignore
            {"name": "name", "analyzer": "norm_en"},
            {"name": "indexed[*]", "analyzer": "norm_en"},
        ],
        name=config.search_index_name,
    )
    db.create_view(
        name=config.search_view,
        view_type="search-alias",
        properties={
            "indexes": [
                {
                    "collection": mo_collection.name,
                    "index": config.search_index_name,
                },
            ]
        },
    )

    # path collection
    graph_service.get_collection(
        db=db,
        name=config.graph_data_path_name,
        if_not_exist=IfNotExistType.CREATE,
        edge=True,
    )
    # path graph
    graph_service.create_graph(
        db=db,
        name=config.graph_data_path_graph_name,
        edge_collection=config.graph_data_path_name,
        from_vertex_collections=[config.graph_data_collection_name],
        to_vertex_collections=[config.graph_data_collection_name],
    )

    # main graph
    graph_service.create_graph(
        db=db,
        name=config.graph_data_graph_name,
        edge_collection=config.graph_data_edge_name,
        from_vertex_collections=[config.graph_data_collection_name],
        to_vertex_collections=[config.graph_data_collection_name],
        if_exist=IfNotExistType.RETURN_NONE,
    )


def init_databases(
    graph_service: GraphService, config: GraphDBConfig, data: list[dict]
):
    for record in data:
        record = DbMainRecord.model_validate(record)
        # recreate database
        if graph_service.sys_db.has_database(name=record.database):
            graph_service.delete_database(name=record.database)
        db = graph_service.get_database(
            record.database, if_not_exist=IfNotExistType.CREATE
        )
        # create collections
        init_collections(graph_service=graph_service, db=db, config=config)
