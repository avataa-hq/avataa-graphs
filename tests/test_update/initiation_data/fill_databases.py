import glob
import json
import os

from arango.database import StandardDatabase

from services.graph import GraphService, IfNotExistType


def fill_database(
    graph_service: GraphService, folder: str, db: StandardDatabase
):
    for file in glob.glob("*.json", root_dir=folder):
        collection_name = file.split(".")[0]
        collection = graph_service.get_collection(
            db=db, name=collection_name, if_not_exist=IfNotExistType.CREATE
        )
        with open(os.path.join(folder, file), "r") as f:
            data = json.load(f)
            collection.import_bulk(
                data,
            )


def fill_databases(graph_service: GraphService, data: list[dict], cwd: str):
    folder = os.path.join(
        cwd, f"{cwd}/tests/test_update/initiation_data/data/databases/"
    )
    main_record_databases = [i["database"] for i in data]
    databases = {
        name
        for name in os.listdir(folder)
        if os.path.isdir(os.path.join(folder, name))
    }
    for database in main_record_databases:
        if database not in databases:
            continue
        db = graph_service.get_database(database)
        db_folder = os.path.join(folder, database)
        fill_database(graph_service=graph_service, folder=db_folder, db=db)
