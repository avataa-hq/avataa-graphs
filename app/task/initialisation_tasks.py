import json

from arango.collection import StandardCollection
from arango.database import StandardDatabase

from config import GraphDBConfig
from services.graph import GraphService, IfNotExistType
from services.inventory import InventoryInterface
from task.models.dto import InitialRecordCreating
from task.models.enums import LinkType, Status
from task.models.incoming_data import InitialRecordCreate, InitialRecordUpdate
from task.models.outgoing_data import InitialRecord


class InitGraphTask:
    def __init__(
        self,
        graph_data: InitialRecordCreate,
        graph_db: GraphService,
        inventory: InventoryInterface,
    ):
        self.graph_data = graph_data
        self.graph = graph_db
        self.inventory = inventory

    def execute(self, recreate=True) -> None:
        record = InitialRecordCreating.model_validate(
            {"status": Status.NEW, **self.graph_data.model_dump()}
        )
        db_name = GraphDBConfig().get_db_name(self.graph_data.tmo_id)
        record.database = db_name
        record = self.create_record(data=record, overwrite=recreate)

        try:
            new_db = self.graph.get_database(
                db_name, if_not_exist=IfNotExistType.CREATE
            )
            self._create_collections(db=new_db, recreate=True)
            tmo_ids = self.fill_tmo_graph(db=new_db)
            record.active_tmo_ids = tmo_ids
            record = self.create_record(data=record, overwrite=True)
        except Exception as e:
            record.status = Status.ERROR
            record.error_description = str(e)
            self.create_record(data=record, overwrite=True)
            raise e

    def check(self):
        def check_in_main_collection():
            sys_db: StandardDatabase = self.graph.sys_db
            main_collection = self.graph.get_collection(
                db=sys_db,
                name=GraphDBConfig().main_graph_collection_name,
                if_not_exist=IfNotExistType.CREATE,
            )
            filters = [
                self.graph_data.model_dump(include={"name"}),
                self.graph_data.model_dump(include={"tmo_id"}),
            ]
            for filt in filters:
                find = main_collection.find(filters=filt, limit=1)
                if not find.empty():
                    raise ValueError(
                        "Record with {} already exists".format(filt)
                    )

        def check_tmo_exists():
            resp = self.inventory.get_tmo_tree(tmo_id=self.graph_data.tmo_id)
            if not resp:
                raise ValueError(
                    "TMO with id {} does not exist in inventory".format(
                        self.graph_data.tmo_id
                    )
                )

        def check_db():
            db_name = GraphDBConfig().get_db_name(self.graph_data.tmo_id)
            if (
                self.graph.get_database(
                    name=db_name, if_not_exist=IfNotExistType.RETURN_NONE
                )
                is not None
            ):
                raise ValueError(
                    "DB with name {} already exists".format(db_name)
                )

        check_in_main_collection()
        check_tmo_exists()
        check_db()

    def create_record(
        self, data: InitialRecordCreating, overwrite: bool = False
    ) -> InitialRecordCreating:
        data = json.loads(
            data.model_dump_json(exclude_none=True, by_alias=True)
        )
        sys_db = self.graph.sys_db
        main_collection_name = GraphDBConfig().main_graph_collection_name
        main_collection = self.graph.get_collection(
            db=sys_db,
            name=main_collection_name,
            if_not_exist=IfNotExistType.CREATE,
        )
        response = main_collection.insert(
            data, return_new=True, overwrite=overwrite, overwrite_mode="replace"
        )
        data = InitialRecordCreating.model_validate(response["new"])
        return data

    def _create_collections(self, db: StandardDatabase, recreate: bool):
        if recreate:
            collections = db.collections()
            for collection in collections:
                if collection["system"]:
                    continue
                db.delete_collection(collection)
        config = GraphDBConfig()
        # tmo
        tmo_collection = self.graph.get_collection(
            db=db,
            name=config.tmo_collection_name,
            if_not_exist=IfNotExistType.CREATE,
        )
        tmo_collection.add_hash_index(fields=["name"], unique=True, sparse=True)

        self.graph.get_collection(
            db=db,
            name=config.tmo_edge_name,
            if_not_exist=IfNotExistType.CREATE,
            edge=True,
        )

        self.graph.create_graph(
            db=db,
            name=config.tmo_graph_name,
            edge_collection=config.tmo_edge_name,
            from_vertex_collections=[config.tmo_collection_name],
            to_vertex_collections=[config.tmo_collection_name],
        )

        self.graph.get_collection(
            db=db,
            name=config.config_collection_name,
            if_not_exist=IfNotExistType.CREATE,
        )

        # mo
        mo_collection = self.graph.get_collection(
            db=db,
            name=config.graph_data_collection_name,
            if_not_exist=IfNotExistType.CREATE,
        )
        mo_collection.add_hash_index(
            fields=["grouped_by_tprm"], unique=False, sparse=True
        )
        mo_collection.add_hash_index(fields=["name"], unique=False, sparse=True)
        mo_collection.add_hash_index(fields=["tmo"], unique=False, sparse=True)

        edge_mo_collection = self.graph.get_collection(
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

        self.graph.get_collection(
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
        # Check arango version
        major_version = self.graph.sys_db.version().split(".")[1]
        if major_version == "11":
            mo_collection.add_inverted_index(
                fields=[  # type: ignore
                    {"name": "name", "analyzer": "norm_en"},
                    {"name": "label", "analyzer": "norm_en"},
                    {"name": "indexed[*]", "analyzer": "norm_en"},
                ],
                name=config.search_index_name,
            )
        elif major_version == "12":
            mo_collection.add_index(
                {
                    "fields": ["name", "label", "indexed[*]"],
                    "type": "inverted",
                    "name": config.search_index_name,
                    "analyzers": ["norm_en"],
                }
            )
        else:
            raise ValueError(
                f"Incorrect Arango version: {self.graph.sys_db.version()}"
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
        self.graph.get_collection(
            db=db,
            name=config.graph_data_path_name,
            if_not_exist=IfNotExistType.CREATE,
            edge=True,
        )
        # path graph
        self.graph.create_graph(
            db=db,
            name=config.graph_data_path_graph_name,
            edge_collection=config.graph_data_path_name,
            from_vertex_collections=[config.graph_data_collection_name],
            to_vertex_collections=[config.graph_data_collection_name],
        )

    def fill_tmo_graph(self, db: StandardDatabase) -> list[int]:
        config = GraphDBConfig()

        def build_element(element: dict) -> dict:
            element = element.copy()
            del element["child"]
            element["_id"] = config.get_tmo_collection_key(element["id"])
            element["_key"] = str(element["id"])
            element["enabled"] = True
            element["params"] = []
            return element

        def create_link(
            _to: int, _from: int, link_type: str, tprm_id: int | None
        ) -> dict:
            return {
                "_from": config.get_tmo_collection_key(_from),
                "_to": config.get_tmo_collection_key(_to),
                "link_type": link_type,
                "enabled": True,
                "tprm_id": tprm_id,
            }

        links: list[dict] = []
        elements = {}
        trees = self.inventory.get_tmo_tree(tmo_id=self.graph_data.tmo_id)
        for tree in trees:
            queue: list[dict] = [tree]
            while queue:
                node = queue.pop()
                queue.extend(node["child"])
                elements[node["id"]] = build_element(node)
                for constraint in node["points_constraint_by_tmo"]:
                    links.append(
                        create_link(
                            _to=constraint,
                            _from=node["id"],
                            link_type=LinkType.POINT_CONSTRAINT.value,
                            tprm_id=None,
                        )
                    )
                if node.get("p_id"):
                    links.append(
                        create_link(
                            _to=node["p_id"],
                            _from=node["id"],
                            link_type="p_id",
                            tprm_id=None,
                        )
                    )

        tmo_collection = self.graph.get_collection(
            db=db, name=config.tmo_collection_name
        )
        tmo_collection.truncate()
        tprms = self.inventory.get_tprms_by_tmo_id(list(elements))
        for tprm in tprms:
            element = elements[tprm["tmo_id"]]
            element["params"].append(tprm)

            if tprm["val_type"] not in {
                LinkType.MO_LINK.value,
                LinkType.TWO_WAY_MO_LINK.value,
            }:
                continue
            if not tprm.get("constraint"):
                continue
            if isinstance(tprm.get("constraint"), list):
                tprm["constraint"] = json.dumps(tprm["constraint"])
            try:
                constraints = json.loads(tprm["constraint"])
                for constraint in constraints:
                    mo_link = create_link(
                        _to=constraint,
                        _from=tprm["tmo_id"],
                        link_type=tprm["val_type"],
                        tprm_id=tprm["id"],
                    )
                    links.append(mo_link)
            except TypeError:
                continue

        elements_existing = {config.get_tmo_collection_key(k) for k in elements}
        filtered_links = filter(
            lambda x: True
            if x["_from"] in elements_existing and x["_to"] in elements_existing
            else False,
            links,
        )

        edge_collection = self.graph.get_collection(
            db=db, name=config.tmo_edge_name
        )
        edge_collection.truncate()
        tmo_collection.insert_many(tuple(elements.values()))
        edge_collection.insert_many(filtered_links)
        return list(elements)


class GraphStatesTask:
    def __init__(self, graph_db: GraphService):
        self.graph_db = graph_db
        self.config = GraphDBConfig()
        self.main_collection: StandardCollection = self.graph_db.get_collection(
            db=self.config.sys_database_name,
            name=self.config.main_graph_collection_name,
            if_not_exist=IfNotExistType.CREATE,
        )

    def execute(self) -> list[InitialRecord]:
        return [
            InitialRecord.model_validate(i) for i in self.main_collection.all()
        ]


class GraphStateUpdateTask:
    def __init__(
        self, graph_db: GraphService, key: str, data: InitialRecordUpdate
    ):
        self.graph_db = graph_db
        self.config = GraphDBConfig()
        self.main_database = self.graph_db.sys_db
        self.main_collection: StandardCollection = self.graph_db.get_collection(
            db=self.main_database,
            name=self.config.main_graph_collection_name,
            if_not_exist=IfNotExistType.CREATE,
        )
        self.key = key
        self.data = data

    def check(self):
        def check_name_exist():
            name_exist_query = """
                FOR doc in @@collection
                    FILTER doc._key != @key
                    FILTER doc.name == @name
                    LIMIT 1
                    RETURN doc
                """
            binds = {
                "@collection": self.main_collection.name,
                "key": self.key,
                "name": self.data.name,
            }
            if list(
                self.main_database.aql.execute(
                    name_exist_query, bind_vars=binds
                )
            ):
                raise ValueError(f"{self.data.name} is exist")

        def check_id_not_exist():
            if not list(self.main_collection.find(filters={"_key": self.key})):
                raise ValueError(f"Key {self.key} not exist")

        check_id_not_exist()
        check_name_exist()

    def execute(self) -> InitialRecord:
        document = {
            "_key": self.key,
            **json.loads(self.data.model_dump_json(by_alias=True)),
        }
        response = self.main_collection.update(
            document=document, return_new=True
        )
        return InitialRecord.model_validate(response["new"])


class DeleteGraphStateTask:
    def __init__(self, graph_db: GraphService, key: str):
        self.graph_db = graph_db
        self.config = GraphDBConfig()
        self.main_database = self.graph_db.sys_db
        self.main_collection: StandardCollection = self.graph_db.get_collection(
            db=self.main_database,
            name=self.config.main_graph_collection_name,
            if_not_exist=IfNotExistType.CREATE,
        )
        self.key = key
        self.config = GraphDBConfig()

    def check(self):
        def check_id_not_exist():
            if not list(self.main_collection.find(filters={"_key": self.key})):
                raise ValueError(f"Key {self.key} not exist")

        check_id_not_exist()

    def execute(self):
        item = self.main_collection.get(self.key)
        if item:
            db_name = item.get("database")
            if db_name:
                self.graph_db.delete_database(db_name)
        self.main_collection.delete(
            document={"_key": self.key}, ignore_missing=True
        )
