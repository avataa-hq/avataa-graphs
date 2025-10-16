from arango import DocumentUpdateError

from config import GraphDBConfig
from services.graph import GraphService, IfNotExistType
from task.models.enums import Status
from task.models.outgoing_data import (
    TmoConfigResponse,
    TmoEdgeUpdate,
    TmoNodeUpdate,
    TmoUpdate,
)
from task.task_abstract import TaskAbstract, TaskChecks


class TmoTask(TaskAbstract, TaskChecks):
    def __init__(self, key: str, graph_db: GraphService):
        TaskAbstract.__init__(self, graph_db=graph_db, key=key)

    def check(self):
        self.check_collection(
            document=self.document, tmo_collection=self.tmo_collection
        )

    def execute(self) -> TmoConfigResponse:
        nodes = list(self.tmo_collection.all())
        [i.pop("_id") for i in nodes]

        edges = list(self.tmo_edge_collection.all())
        [i.pop("_id") for i in edges]

        start_node = self.config.get_tmo_collection_key(self.document.tmo_id)

        group_by = self.group_by_tprm_ids
        if not group_by:
            group_by = []

        start_from = self.config_collection.get({"_key": "start_from"})
        # Adding groups
        if group_by:
            group_by_dict: dict[int, dict | None] = {i: None for i in group_by}

            for node in nodes:
                params = node["params"]
                if not params:
                    del node["params"]
                    continue
                for param in params:
                    if param["id"] in group_by_dict:
                        group_by_dict[param["id"]] = param

            edges_dict = {}
            other_edges = []
            for edge in edges:
                if edge["link_type"] != "p_id":
                    other_edges.append(edge)
                    continue
                _from = edge["_from"]
                _from = int(_from.split("/")[1])
                edges_dict[_from] = edge
            for k, v in group_by_dict.items():
                if v is None:
                    continue
                new_node_key = f"{v['tmo_id']}_{v['id']}"
                new_node_id = (
                    f"{self.config.tmo_collection_name}/{new_node_key}"
                )

                new_node = {
                    "_id": new_node_id,
                    "_key": new_node_key,
                    "name": v["name"],
                    "id": new_node_key,
                    "virtual": True,
                    "global_uniqueness": True,
                    "materialize": False,
                    "point_tmo_const": [],
                    "enabled": True,
                    "params": [v] if v else [],
                    "is_grouped": True,
                }
                nodes.append(new_node)

                edge: dict | None = edges_dict.get(v["tmo_id"], None)

                if edge:
                    new_edge = edge
                    new_edge_key = (
                        f"{edge['_key'].rsplit('_', maxsplit=1)[0]}_{k}"
                    )
                    new_edge["_key"] = new_edge_key

                    if new_edge["link_type"] != "p_id":
                        new_edge["_from"] = new_edge["_to"]
                        new_edge["group_by_tprm"] = k
                    else:
                        new_edge["_to_last"] = new_edge["_to"]
                        new_edge["link_type"] = "group_by_tprm"
                    new_edge["_to"] = new_node_id
                    edges_dict[v["tmo_id"]] = new_edge

                    other_edge = new_edge.copy()
                    other_edge.pop("_to_last")
                    other_edges.append(other_edge)
                else:
                    new_edge_key = f"0_{new_node_key}_{k}"
                    new_edge = {"_key": new_edge_key}
                    new_edge["group_by_tprm"] = k
                    new_edge["_from"] = new_node_id
                    new_edge["_to"] = new_node_id.rsplit("_", maxsplit=1)[0]
                    new_edge["_to_last"] = new_node_id
                    new_edge["link_type"] = "group_by_tprm"
                    new_edge["enabled"] = True
                    edges_dict[v["tmo_id"]] = new_edge

            for edge in edges_dict.values():
                if not edge:
                    continue
                if "_to_last" in edge:
                    edge["_key"] = edge["_key"].rsplit("_", maxsplit=1)[0]
                    edge["_from"] = edge["_to"]
                    edge["_to"] = edge.pop("_to_last")
                    edge["link_type"] = "p_id"
                other_edges.append(edge)
            edges = other_edges

        return TmoConfigResponse.model_validate(
            dict(
                nodes=nodes,
                edges=edges,
                start_node_key=start_node,
                group_by_tprms=group_by,
                start_from_tmo_id=start_from.get("tmo_id", self.document.tmo_id)
                if start_from
                else self.document.tmo_id,
                start_from_tprm_id=start_from.get("tprm_id", None)
                if start_from
                else None,
                trace_tmo_id=self.trace_tmo_id,
                trace_tprm_id=self.trace_tprm_id,
                delete_orphan_branches=self.delete_orphan_branches_status,
            )
        )


class TmoUpdateTask(TaskAbstract, TaskChecks):
    def __init__(self, key: str, data: TmoUpdate, graph_db: GraphService):
        TaskAbstract.__init__(self, graph_db=graph_db, key=key)
        self.data = data

    def check(self):
        if self.data.nodes or self.data.edges:
            self.check_status(
                document=self.document,
                possible_status=[Status.NEW, Status.COMPLETE, Status.ERROR],
            )
        self.check_collection(
            tmo_collection=self.tmo_collection, document=self.document
        )
        if self.data.nodes:
            self.check_nodes(
                keys=list({str(i.key) for i in self.data.nodes}),
                main_collection_name=self.tmo_collection.name,
                database=self.database,
            )
        if self.data.edges:
            self.check_edges(
                keys=list({str(i.key) for i in self.data.edges}),
                main_edge_collection_name=self.tmo_edge_collection.name,
                database=self.database,
            )
        if self.data.group_by_tprms:
            self.check_group_by(
                data=self.data,
                start_from=self.config.get_tmo_collection_key(
                    self.document.tmo_id
                ),
                tmo_graph_name=self.config.tmo_graph_name,
                tmo_collection_name=self.config.tmo_collection_name,
                database=self.database,
            )

        self.check_start_from(
            data=self.data,
            database=self.database,
            tmo_collection_name=self.tmo_collection.name,
            settings_collection=self.config_collection,
        )
        self.check_trace(
            data=self.data,
            document=self.document,
            tmo_collection=self.tmo_collection,
        )

    def _update_nodes(self, nodes: list[TmoNodeUpdate] | None):
        if not nodes:
            return
        nodes_dict = {str(i.key): i.enabled for i in nodes}
        query = """
            FOR doc in @@collection
                FILTER doc._key IN @keys
                RETURN doc
        """
        binds = {
            "@collection": self.tmo_collection.name,
            "keys": list(nodes_dict),
        }
        new_nodes = [
            {"_id": i["_id"], "enabled": nodes_dict[i["_key"]]}
            for i in self.database.aql.execute(query=query, bind_vars=binds)
        ]
        response = self.tmo_collection.update_many(new_nodes)

        for i in response:
            if isinstance(i, DocumentUpdateError):
                raise ValueError("Node update error") from DocumentUpdateError

    def _update_edges(self, edges: list[TmoEdgeUpdate] | None):
        if not edges:
            return
        edges_dict = {str(i.key): i.enabled for i in edges}
        query = """
            FOR doc in @@edgeCollection
                FILTER doc._key IN @keys
                FOR doc2 in @@edgeCollection
                    FILTER doc2._from == doc._from
                    FILTER doc2.link_type == doc.link_type
                    FILTER doc2.tprm_id == doc.tprm_id
                    RETURN {"tmp_key": doc._key, "_id": doc2._id}
        """
        binds = {
            "@edgeCollection": self.tmo_edge_collection.name,
            "keys": list(edges_dict),
        }
        new_edges = [
            {"_id": i["_id"], "enabled": edges_dict[i["tmp_key"]]}
            for i in self.database.aql.execute(query=query, bind_vars=binds)
        ]
        response = self.tmo_edge_collection.update_many(
            new_edges, return_new=True
        )

        for i in response:
            if isinstance(i, DocumentUpdateError):
                raise ValueError("Edge creation error") from DocumentUpdateError

    def disable_child_nodes(self, start_from: str):
        query = """
            FOR doc in @@tmoCollection
            FILTER doc.enabled == True

            LET path = (FOR v, e IN ANY SHORTEST_PATH
                doc._id TO @startFrom
                GRAPH @tmoGraph

                FILTER v[*].enabled ALL == True
                FILTER e[*].enabled ALL == True

                RETURN e)

            FILTER LENGTH(path) == 0

            UPDATE doc._key
            WITH {
                enabled: False
                } in @@tmoCollection
            RETURN doc._key
        """
        binds = {
            "startFrom": start_from,
            "tmoGraph": self.config.tmo_graph_name,
            "@tmoCollection": self.tmo_collection.name,
        }
        self.database.aql.execute(query=query, bind_vars=binds)

    def update_active_tmos(self, doc: dict):
        query = """
            FOR doc in @@tmoCollection
                RETURN doc.tmo
        """
        binds = {
            "@tmoCollection": self.tmo_collection.name,
        }
        doc["active_tmo_ids"] = list(
            self.database.aql.execute(query=query, bind_vars=binds)
        )
        main_collection_name = GraphDBConfig().main_graph_collection_name
        main_collection = self.graph_db.get_collection(
            db=self.sys_db,
            name=main_collection_name,
            if_not_exist=IfNotExistType.CREATE,
        )
        main_collection.insert(doc, overwrite=True, overwrite_mode="replace")

    def update_group_by(self, group_by: list[int] | None):
        data = {"_key": "group_by", "tprms": group_by}
        self.config_collection.insert(
            data, overwrite=True, overwrite_mode="replace"
        )

    def update_trace_tmo_id(self, tmo_id: int | None):
        if not tmo_id:
            return
        trace_item = {"_key": "trace_tmo_id", "tmo_id": tmo_id}
        self.config_collection.insert(
            document=trace_item, overwrite=True, overwrite_mode="replace"
        )

    def update_trace_tprm_id(self, tprm_id: int | None):
        trace_item = {"_key": "trace_tprm_id", "tprm_id": tprm_id}
        self.config_collection.insert(
            document=trace_item, overwrite=True, overwrite_mode="replace"
        )

    def update_start_from(self, tmo_id: int | None, tprm_id: int | None):
        data = {"_key": "start_from"}
        if tmo_id is not None:
            data["tmo_id"] = tmo_id
        if tprm_id is not None:
            data["tprm_id"] = tprm_id
        self.config_collection.insert(
            data, overwrite=True, overwrite_mode="replace"
        )

    def delete_orphan_branches(self, delete_orphan_branches: bool):
        if delete_orphan_branches is None:
            return
        data = {
            "_key": "delete_orphan_branches",
            "delete_orphan_branches": delete_orphan_branches,
        }
        self.config_collection.insert(
            data, overwrite=True, overwrite_mode="replace"
        )

    def clean_next_step(self):
        if (
            not self.data.group_by_tprms
            and not self.data.edges
            and not self.data.nodes
        ):
            return

        self.document.status = Status.NEW
        self.document.error_description = ""

        main_collection_name = GraphDBConfig().main_graph_collection_name
        main_collection = self.graph_db.get_collection(
            db=self.sys_db,
            name=main_collection_name,
            if_not_exist=IfNotExistType.CREATE,
        )
        response = main_collection.insert(
            self.document.model_dump(mode="json", by_alias=True),
            return_new=True,
            overwrite=True,
            overwrite_mode="replace",
        )
        return response["new"]

    def execute(self):
        try:
            data_keys = set(self.data.model_dump(exclude_unset=True).keys())
            if "nodes" in data_keys:
                self._update_nodes(nodes=self.data.nodes)
            if "edges" in data_keys:
                self._update_edges(edges=self.data.edges)
            if "group_by_tprms" in data_keys:
                self.update_group_by(group_by=self.data.group_by_tprms)
            if "trace_tmo_id" in data_keys:
                self.update_trace_tmo_id(tmo_id=self.data.trace_tmo_id)
                if "trace_tprm_id" in data_keys:
                    self.update_trace_tprm_id(tprm_id=self.data.trace_tprm_id)
            if (
                "start_from_tmo_id" in data_keys
                or "start_from_tprm_id" in data_keys
            ):
                self.update_start_from(
                    tmo_id=self.data.start_from_tmo_id,
                    tprm_id=self.data.start_from_tprm_id,
                )
            if "delete_orphan_branches" in data_keys:
                self.delete_orphan_branches(
                    delete_orphan_branches=self.data.delete_orphan_branches
                )
            self.disable_child_nodes(
                start_from=self.config.get_tmo_collection_key(
                    self.document.tmo_id
                )
            )
            self.clean_next_step()

            subtask = TmoTask(key=self.key, graph_db=self.graph_db)
            return subtask.execute()
        except ValueError as e:
            self.document.status = Status.ERROR
            self.document.error_description = str(e)

            main_collection_name = GraphDBConfig().main_graph_collection_name
            main_collection = self.graph_db.get_collection(
                db=self.sys_db,
                name=main_collection_name,
                if_not_exist=IfNotExistType.CREATE,
            )
            main_collection.insert(
                self.document.model_dump(mode="json", by_alias=True),
                overwrite=True,
                overwrite_mode="replace",
            )
            raise e
