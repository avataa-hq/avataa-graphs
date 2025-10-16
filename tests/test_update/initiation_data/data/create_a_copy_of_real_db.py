import json
from json import JSONDecodeError
from pathlib import Path

from config import ArangoConfig, GraphDBConfig
from services.graph import GraphService
from task.models.dto import DbMainRecord
from task.task_abstract import TaskAbstract
from updater.main import get_new_graph_db


def get_main_records(
    graph_db: GraphService, graph_config: GraphDBConfig
) -> list[DbMainRecord]:
    main_collection = graph_db.sys_db.collection(
        graph_config.main_graph_collection_name
    )
    records = [DbMainRecord.model_validate(i) for i in main_collection.all()]
    return records


def add_main_record_to_file(main_record: DbMainRecord):
    with open("main_records.json", "r") as f:
        try:
            existing_record = json.load(f)
        except JSONDecodeError:
            existing_record = []
    existing_record.append(main_record.model_dump(mode="json", by_alias=True))
    with open("main_records.json", "w") as f:
        json.dump(existing_record, f, indent=3)


def add_db_collections_to_files(document: DbMainRecord, task: TaskAbstract):
    folder_url = f"./databases/{document.database}"
    Path(folder_url).mkdir(parents=True, exist_ok=True)
    for collection_name in task.database.collections():
        if collection_name["system"]:
            continue
        collection = task.graph_db.get_collection(
            db=task.database, name=collection_name["name"]
        )
        data = collection.all()
        file_name = f"{folder_url}/{collection_name['name']}.json"
        with open(file_name, "w") as f:
            json.dump(list(data), f, indent=3)


def create_copy_of_one_db(task: TaskAbstract, document: DbMainRecord):
    add_main_record_to_file(main_record=document)
    add_db_collections_to_files(document=document, task=task)


def truncate_main_records():
    open("main_records.json", "w").close()


def create_a_copy_of_real_db(
    graph_db: GraphService, graph_config: GraphDBConfig, db_names: set[str]
):
    main_records = get_main_records(
        graph_db=graph_db, graph_config=graph_config
    )
    truncate_main_records()
    for main_record in main_records:
        if db_names and main_record.name not in db_names:
            continue
        task = TaskAbstract(graph_db=graph_db, key=str(main_record.id))
        create_copy_of_one_db(task=task, document=main_record)


if __name__ == "__main__":
    db_names: set[str] = {"TN Dataset", "Barcelona"}
    arango_config = ArangoConfig()
    graph_config = GraphDBConfig()
    graph_db = get_new_graph_db(config=arango_config)
    create_a_copy_of_real_db(
        graph_db=graph_db, graph_config=graph_config, db_names=db_names
    )
