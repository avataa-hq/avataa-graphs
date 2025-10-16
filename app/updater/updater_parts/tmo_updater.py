from arango import DocumentInsertError

from services.graph import GraphService
from services.inventory import InventoryInterface
from task.helpers.drop_tmo_from_mo_collection import drop_tmo_from_mo_collection
from task.initialisation_tasks import DeleteGraphStateTask
from task.models.dto import DbTmoNode, TmoEdge
from task.models.enums import LinkType
from task.models.errors import ValidationError
from task.models.incoming_data import TMO
from updater.updater_parts.updater_abstract import ItemUpdaterAbstract
from updater.updater_parts.updater_task_abstract import UpdaterTaskAbstract


class TmoMainUpdater(ItemUpdaterAbstract, UpdaterTaskAbstract):
    """
    Обновление ТМО в главной базе данных с записями графов
    """

    def __init__(self, graph_db: GraphService, key: str):
        ItemUpdaterAbstract.__init__(self)
        UpdaterTaskAbstract.__init__(self, graph_db=graph_db, key=key)

    def _update(self, items: list[TMO]):
        pass

    def _delete(self, items: list[TMO]):
        document = self.document
        main_item = document.tmo_id
        item_ids = {i.tmo_id for i in items}
        if main_item in item_ids:
            task = DeleteGraphStateTask(graph_db=self.graph_db, key=self.key)
            task.execute()
            exit()
        record_items = list(set(document.active_tmo_ids).difference(item_ids))
        document.active_tmo_ids = record_items
        record = document.model_dump(mode="json", by_alias=True)
        self.system_main_collection.update(document=record, keep_none=True)

    def _create(self, items: list[TMO]):
        document = self.document
        item_ids = {i.tmo_id for i in items}
        item_ids.update(document.active_tmo_ids)
        document.active_tmo_ids = list(item_ids)
        record = document.model_dump(mode="json", by_alias=True)
        self.system_main_collection.update(document=record, keep_none=True)


class TmoSettingsUpdater(ItemUpdaterAbstract, UpdaterTaskAbstract):
    """Обновление ТМО в настройках графа"""

    def __init__(self, graph_db: GraphService, key: str):
        ItemUpdaterAbstract.__init__(self)
        UpdaterTaskAbstract.__init__(self, graph_db=graph_db, key=key)

    def __delete_from_trace(self, tmo_ids: list[int]):
        trace_id = self.trace_tmo_id
        if not trace_id:
            return
        if trace_id in tmo_ids:
            self.config_collection.delete(
                {"_key": "trace_tmo_id"}, ignore_missing=True
            )
            self.config_collection.delete(
                {"_key": "trace_tprm_id"}, ignore_missing=True
            )
            self._trace_tmo_id = None

    def __delete_from_start(self, tmo_ids: list[int]):
        start_from = self.config_collection.get({"_key": "start_from"})
        if start_from:
            start_from = start_from["tmo_id"]
        if not start_from:
            return
        if start_from in tmo_ids:
            self.config_collection.delete(
                {"_key": "start_from"}, ignore_missing=True
            )

    def __delete_group_by(self, tmo_ids: list[int]):
        group_by = self.config_collection.get({"_key": "group_by"})
        if group_by:
            group_by = group_by["tprms"]
        if not group_by:
            return
        query = """
            FOR node IN @@tmoCollection
                FILTER node.id IN @tmoIds
                FILTER NOT_NULL(node.params)
                FOR param IN node.params
                    FILTER param.id IN @tprmIds
                    RETURN {"tmoId": node.id, "tprmId": param.id}
        """
        binds = {
            "@tmoCollection": self.tmo_collection.name,
            "tmoIds": tmo_ids,
            "tprmIds": group_by,
        }
        response = list(self.database.aql.execute(query=query, bind_vars=binds))
        if not response:
            return
        exclude_tprms = set(i["tprmId"] for i in response)
        group_by = [i for i in group_by if i not in exclude_tprms]
        record = {"_key": "group_by", "tprms": group_by}
        self.config_collection.update(record, check_rev=False)
        # удалить группировку из таблицы с МО и пересоздать связи
        for filtered_exclude_row in response:
            drop_tmo_from_mo_collection(
                tmo_id=filtered_exclude_row["tmoId"],
                tprm_ids=[filtered_exclude_row["tprmId"]],
                task=self,
            )

    def _delete(self, items: list[TMO]):
        tmo_ids = [i.tmo_id for i in items]
        self.__delete_from_trace(tmo_ids=tmo_ids)
        self.__delete_from_start(tmo_ids=tmo_ids)
        self.__delete_group_by(tmo_ids=tmo_ids)

    def _create(self, items: list[TMO]):
        pass

    def _update(self, items: list[TMO]):
        pass


class TmoTmoUpdater(ItemUpdaterAbstract, UpdaterTaskAbstract):
    """Обновление ТМО в части графа с ТМО"""

    def __init__(
        self, graph_db: GraphService, inventory: InventoryInterface, key: str
    ):
        ItemUpdaterAbstract.__init__(self)
        UpdaterTaskAbstract.__init__(self, graph_db=graph_db, key=key)
        self._inventory = inventory

    def __find_all_children(
        self, tmo_ids: list[int], recursive: bool = True
    ) -> list[DbTmoNode]:
        query = """
            FOR edge IN @@tmoEdgeCollection
                FILTER edge._to IN @tmoIds
                FILTER edge.link_type == "p_id"
                FOR node IN @@tmoCollection
                    FILTER node.id == edge._from
                    RETURN node
        """
        binds = {
            "@tmoCollection": self.tmo_collection.name,
            "@tmoEdgeCollection": self.tmo_edge_collection.name,
            "tmoIds": tmo_ids,
        }
        result = [
            DbTmoNode.model_validate(i)
            for i in self.database.aql.execute(query=query, bind_vars=binds)
        ]
        if recursive and result:
            child_result = self.__find_all_children(
                tmo_ids=[i.tmo_id for i in result], recursive=recursive
            )
            result.extend(child_result)
        return result

    def __get_db_tmo_nodes_dict(
        self, tmo_ids: list[int]
    ) -> dict[int, DbTmoNode]:
        query = """
            FOR node IN @@tmoCollection
                FILTER node.id IN @tmoIds
                RETURN node
        """
        binds = {"@tmoCollection": self.tmo_collection.name, "tmoIds": tmo_ids}
        response = self.database.aql.execute(query=query, bind_vars=binds)
        results = {}
        for raw_node in response:
            item = DbTmoNode.model_validate(raw_node)
            results[item.tmo_id] = item
        return results

    def _update(self, items: list[TMO]):
        db_tmo_nodes = self.__get_db_tmo_nodes_dict(
            tmo_ids=[i.tmo_id for i in items]
        )
        to_create_list = []
        to_delete_list = []
        to_update_list = []

        links_to_create = []
        for item in items:
            db_item = db_tmo_nodes.get(item.tmo_id, None)
            # new item
            if not db_item:
                to_create_list.append(item)
                continue

            # parent changed
            root_tmo_id = self.document.tmo_id
            if root_tmo_id != item.tmo_id and item.p_id != db_item.p_id:
                if not item.p_id:
                    to_delete_list.append(item)
                    continue
                else:
                    parent_node = self.__get_db_tmo_nodes_dict(
                        tmo_ids=[item.p_id]
                    )
                    if not parent_node:
                        to_delete_list.append(item)
                        continue
                    else:
                        parent_node = parent_node[item.p_id]
                        # delete old p_id link
                        query = """
                            FOR edge IN @@tmoEdgeCollection
                                FILTER edge._from == @nodeId
                                FILTER edge.link_type == "p_id"
                                REMOVE edge._key IN @@tmoEdgeCollection
                        """
                        binds = {
                            "@tmoEdgeCollection": self.tmo_edge_collection.name,
                            "nodeId": db_item.id,
                        }
                        self.database.aql.execute(query=query, bind_vars=binds)
                        # create and insert new p_id link
                        link = TmoEdge(
                            _from=db_item.id,
                            _to=parent_node.id,
                            link_type=LinkType.P_ID,
                            enabled=False,
                            tprm_id=None,
                        )
                        links_to_create.append(link)

            # constraint changed
            new_constraint = (
                set(item.points_constraint_by_tmo)
                if item.points_constraint_by_tmo
                else set()
            )
            db_constraint = (
                set(db_item.points_constraint_by_tmo)
                if db_item.points_constraint_by_tmo
                else set()
            )
            if new_constraint != db_constraint:
                # create new links
                new_constraint_items = new_constraint.difference(db_constraint)
                if new_constraint_items:
                    other_side_nodes = self.__get_db_tmo_nodes_dict(
                        tmo_ids=list(new_constraint_items)
                    )
                    for other_side_node in other_side_nodes.values():
                        link = TmoEdge(
                            _from=db_item.id,
                            _to=other_side_node.id,
                            link_type=LinkType.POINT_CONSTRAINT,
                            enabled=False,
                            tprm_id=None,
                        )
                        links_to_create.append(link)

                # delete old links
                old_constraint_items = db_constraint.difference(
                    new_constraint_items
                )
                if old_constraint_items:
                    other_side_node_ids = [
                        self.config.get_tmo_collection_key(i)
                        for i in old_constraint_items
                    ]
                    query = """
                        FOR edge IN @@tmoEdgeCollection
                            FILTER edge._from == @nodeId
                            FILTER edge.link_type == "point_tmo_constraint"
                            FILTER edge._to IN @otherSideNodeIds
                            REMOVE edge._key IN @@tmoEdgeCollection
                    """
                    binds = {
                        "@tmoEdgeCollection": self.tmo_edge_collection.name,
                        "nodeId": db_item.id,
                        "otherSideNodeIds": other_side_node_ids,
                    }
                    self.database.aql.execute(query=query, bind_vars=binds)
            db_item = db_item.model_copy(
                update=item.model_dump(
                    by_alias=True, exclude={"tmo_id", "params"}
                )
            )
            to_update_list.append(db_item)
        if to_update_list:
            self.tmo_collection.update_many(
                [
                    i.model_dump(mode="json", by_alias=True)
                    for i in to_update_list
                ]
            )
        if links_to_create:
            self.tmo_edge_collection.insert_many(
                [
                    i.model_dump(by_alias=True, mode="json")
                    for i in links_to_create
                ]
            )
        if to_create_list:
            self._create(items=to_create_list, create_children=True)
        if to_delete_list:
            self._delete(items=to_delete_list, delete_children=True)

    def _delete(self, items: list[TMO], delete_children: bool = True):
        # Удаление главного ТМО обрабатывается в TmoMainUpdater. Тут только дочерние.
        # Собираем ТМО в кучу
        tmo_ids = [i.tmo_id for i in items]
        if delete_children:
            child_tmo_nodes = self.__find_all_children(
                tmo_ids=tmo_ids, recursive=True
            )
            child_tmo_ids = [i.tmo_id for i in child_tmo_nodes]
            tmo_ids.extend(child_tmo_ids)

        # Удаляем из коллекции ТМО
        del_tmo_nodes_query = """
            FOR node IN @@tmoCollection
                FILTER node.id IN @tmoIds
                REMOVE node._key IN @@tmoCollection
                FOR edge IN @@tmoEdgeCollection
                    FILTER edge._from == node._id OR edge._to == node._id
                    REMOVE edge._key IN @@tmoEdgeCollection
        """
        binds = {
            "@tmoCollection": self.tmo_collection.name,
            "tmoIds": tmo_ids,
            "@tmoEdgeCollection": self.tmo_edge_collection.name,
        }
        self.database.aql.execute(query=del_tmo_nodes_query, bind_vars=binds)

        # Удаляем из коллекции МО
        del_mo_nodes_query = """
            FOR node IN @@mainCollection
                FILTER node.tmo IN @tmoIds
                REMOVE node._key IN @@mainCollection
                FOR edge IN @@mainEdgeCollection
                    FILTER edge._from == node._id OR edge._to == node._id
                    REMOVE edge._key IN @@mainEdgeCollection
        """
        binds = {
            "@mainCollection": self.main_collection.name,
            "@mainEdgeCollection": self.main_edge_collection.name,
            "tmoIds": tmo_ids,
        }
        self.database.aql.execute(query=del_mo_nodes_query, bind_vars=binds)

    @staticmethod
    def __unpack_tmo_tree(tree: list[dict]) -> list[TMO]:
        results = []
        queue = tree.copy()
        while queue:
            item = queue.pop()
            children = item["child"]
            if children:
                queue.extend(children)
            tmo = TMO.model_validate(item)
            results.append(tmo)
        return results

    def __get_parents_dict(self, tmo_ids: list[int]) -> dict[int, DbTmoNode]:
        query = """
            FOR edge IN @@tmoEdgeCollection
                FILTER edge._from IN @tmoIds
                FILTER edge.link_type == "p_id"
                FOR node IN @@tmoCollection
                    FILTER node._id == edge._to
                    RETURN {"tmoId": edge._from, "parentNode": node}
        """
        binds = {
            "tmoIds": [
                self.config.get_tmo_collection_key(tmo_id=i) for i in tmo_ids
            ],
            "@tmoCollection": self.tmo_collection.name,
            "@tmoEdgeCollection": self.tmo_edge_collection.name,
        }
        response = self.database.aql.execute(query=query, bind_vars=binds)
        results = {}
        for row in response:
            tmo_id = int(row["tmoId"].split("/")[1])
            parent = DbTmoNode.model_validate(row["parentNode"])
            results[tmo_id] = parent
        return results

    def __create_items(self, items: list[TMO], parent: DbTmoNode):
        parents_cache: dict[int, DbTmoNode] = {}
        if parent:
            parents_cache[parent.tmo_id] = parent
        # Create nodes
        tmo_nodes = []
        for item in items:
            item_dict = item.model_dump(by_alias=True, mode="json")
            item_dict["_id"] = self.config.get_tmo_collection_key(item.tmo_id)
            item_dict["_key"] = str(item.tmo_id)
            item_dict["enabled"] = False
            item_dict["params"] = []
            tmo_nodes.append(item_dict)
        db_tmo_nodes = []
        for item in self.tmo_collection.insert_many(tmo_nodes, return_new=True):
            if isinstance(item, DocumentInsertError):
                raise ValidationError("Node insert error" + str(item))
            new_item = item["new"]
            db_tmo_node = DbTmoNode.model_validate(new_item)
            db_tmo_nodes.append(db_tmo_node)
            parents_cache[db_tmo_node.tmo_id] = db_tmo_node
        # Create edges
        tmo_edges = []
        for db_tmo_node in db_tmo_nodes:
            parent = parents_cache.get(db_tmo_node.tmo_id, None)
            if not parent:
                continue
            tmo_edge = TmoEdge(
                _from=db_tmo_node.id,
                _to=parent.id,
                link_type="p_id",
                enabled=False,
                tprm_id=None,
            )
            tmo_edges.append(tmo_edge)
        for edge in self.tmo_edge_collection.insert_many(
            [i.model_dump(by_alias=True, mode="json") for i in tmo_edges]
        ):
            if isinstance(edge, DocumentInsertError):
                raise ValidationError("Edge insert error" + str(edge))

        # Create point edges
        point_edges = []
        constraints_flat = []
        for node in db_tmo_nodes:
            if not node.points_constraint_by_tmo:
                continue
            constraints_flat.extend(node.points_constraint_by_tmo)
        if constraints_flat:
            tmo_nodes_dict = self.__get_db_tmo_nodes_dict(
                tmo_ids=constraints_flat
            )
            for db_tmo_node in db_tmo_nodes:
                parent = tmo_nodes_dict.get(db_tmo_node.tmo_id, None)
                if not parent:
                    continue
                tmo_edge = TmoEdge(
                    _from=db_tmo_node.id,
                    _to=parent.id,
                    link_type=LinkType.POINT_CONSTRAINT.value,
                    enabled=False,
                    tprm_id=None,
                )
                point_edges.append(tmo_edge)
            for edge in self.tmo_edge_collection.insert_many(
                [i.model_dump(by_alias=True, mode="json") for i in point_edges]
            ):
                if isinstance(edge, DocumentInsertError):
                    raise ValidationError("Edge insert error" + str(edge))

    def _create(self, items: list[TMO], create_children: bool = False):
        parents_dict = self.__get_parents_dict([i.tmo_id for i in items])
        for item in items:
            if create_children:
                tree = self._inventory.get_tmo_tree(tmo_id=item.tmo_id)
                item_list = self.__unpack_tmo_tree(tree=tree)
            else:
                if not item.points_constraint_by_tmo:
                    item.points_constraint_by_tmo = (
                        self._inventory.get_point_tmo_const(item.tmo_id)
                    )
                item_list = [item]
            parent_node = parents_dict.get(item.tmo_id, None)
            self.__create_items(items=item_list, parent=parent_node)
