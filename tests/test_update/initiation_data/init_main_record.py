from config import GraphDBConfig
from services.graph import GraphService, IfNotExistType


def init_main_records(
    graph_service: GraphService, config: GraphDBConfig, data: list[dict]
):
    main_collection = graph_service.get_collection(
        graph_service.sys_db,
        name=config.main_graph_collection_name,
        if_not_exist=IfNotExistType.CREATE,
    )
    main_collection.truncate()
    main_collection.import_bulk(documents=data, sync=True)
