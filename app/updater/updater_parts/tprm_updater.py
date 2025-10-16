from ast import literal_eval
import json
from typing import Iterable

from services.graph import GraphService
from services.inventory import InventoryInterface
from task.helpers.drop_tmo_from_mo_collection import drop_tmo_from_mo_collection
from task.models.dto import DbTmoEdge, DbTmoNode, TmoEdge
from task.models.enums import LinkType
from task.models.incoming_data import TPRM
from updater.updater_parts.updater_abstract import ItemUpdaterAbstract
from updater.updater_parts.updater_task_abstract import UpdaterTaskAbstract


class TprmSettingUpdater(ItemUpdaterAbstract, UpdaterTaskAbstract):
    def __init__(self, graph_db: GraphService, key: str):
        ItemUpdaterAbstract.__init__(self)
        UpdaterTaskAbstract.__init__(self, graph_db=graph_db, key=key)

    def _update(self, items: list[TPRM]):
        pass

    def __delete_group_by(self, items_dict: dict[int, TPRM]):
        group_by = self.group_by_tprm_ids
        if not group_by:
            return
        group_intersection = set(items_dict).intersection(group_by)
        if not group_intersection:
            return
        new_group_by = [i for i in group_by if i not in group_intersection]
        record = {
            "_key": "group_by",
            "tprms": new_group_by,
        }
        self.config_collection.update(record, check_rev=False)
        for item_id in group_intersection:
            item = items_dict[item_id]
            drop_tmo_from_mo_collection(
                task=self, tmo_id=item.tmo_id, tprm_ids=[item.id]
            )

    def __delete_trace_tprm(self, items_dict: dict[int, TPRM]):
        trace = self.trace_tprm_id
        if not trace:
            return
        if trace not in items_dict:
            return
        self.config_collection.delete({"_key": "trace_tprm_id"})

    def __delete_start_tprm(self, items_dict: dict[int, TPRM]):
        start_tprm = self.start_from_tprm
        if not start_tprm:
            return
        if start_tprm not in items_dict:
            return
        start_config = self.config_collection.get({"_key": "start_from"})
        if not start_config:
            return
        start_config["tprm_id"] = None
        self.config_collection.update(start_config)

    def _delete(self, items: list[TPRM]):
        items_dict = {i.id: i for i in items}
        self.__delete_group_by(items_dict=items_dict)
        self.__delete_trace_tprm(items_dict=items_dict)
        self.__delete_start_tprm(items_dict=items_dict)

    def _create(self, items: list[TPRM]):
        pass


class TprmTmoUpdater(ItemUpdaterAbstract, UpdaterTaskAbstract):
    def __init__(
        self, graph_db: GraphService, key: str, inventory: InventoryInterface
    ):
        ItemUpdaterAbstract.__init__(self)
        UpdaterTaskAbstract.__init__(self, graph_db=graph_db, key=key)
        self.inventory = inventory

    def _update(self, items: list[TPRM]):
        tmo_ids = list(set(i.tmo_id for i in items))
        db_tmo_nodes = self.__get_db_tmo_nodes_dict(tmo_ids=tmo_ids)
        tmos_to_update = []
        items_to_create = []
        links_to_delete = []
        links_to_create = []
        for item in items:
            if isinstance(item.constraint, list):
                item.constraint = json.dumps(item.constraint)
            tmo_node = db_tmo_nodes.get(item.tmo_id, None)
            if not tmo_node:
                continue
            old_item: TPRM | None = None
            params = []
            for param in tmo_node.params:
                if param.id != item.id:
                    params.append(param)
                else:
                    old_item = param
            if not old_item:
                items_to_create.append(item)
                continue
            tmo_node.params = params
            tmos_to_update.append(tmo_node)
            if item.val_type == "mo_link":
                if not item.constraint:
                    item.constraint = json.dumps(
                        self.inventory.get_tprm_const(tprm_id=item.id)
                    )
                old_constraint_list: list[int] = (
                    literal_eval(old_item.constraint)
                    if old_item and old_item.constraint
                    else []
                )
                new_constraint_list = []
                if item.constraint:
                    parsed_constraint = json.loads(item.constraint)
                    if isinstance(parsed_constraint, int):
                        new_constraint_list.append(parsed_constraint)
                    elif isinstance(parsed_constraint, list):
                        new_constraint_list += parsed_constraint
                    else:
                        raise ValueError("Unexpected item.constraint")
                new_constraints: set[int] = set(new_constraint_list).difference(
                    old_constraint_list
                )
                old_constraints: set[int] = set(old_constraint_list).difference(
                    new_constraint_list
                )
                if new_constraints:
                    other_side_tmo_nodes = self.__get_db_tmo_nodes_dict(
                        tmo_ids=list(new_constraints)
                    )
                    mo_links = self._create_mo_links(
                        node=tmo_node,
                        other_side_tmo_nodes=list(
                            other_side_tmo_nodes.values()
                        ),
                        tprm=item,
                    )
                    links_to_create.extend(mo_links)
                if old_constraints:
                    other_side_tmo_nodes = self.__get_db_tmo_nodes_dict(
                        tmo_ids=list(old_constraints)
                    )
                    mo_links = self._delete_mo_links(
                        node=tmo_node,
                        other_side_tmo_nodes=list(
                            other_side_tmo_nodes.values()
                        ),
                        tprm=item,
                    )
                    links_to_delete.extend(mo_links)
        if tmos_to_update:
            self.tmo_collection.update_many(
                [
                    i.model_dump(by_alias=True, mode="json")
                    for i in tmos_to_update
                ]
            )
        if links_to_delete:
            self.tmo_edge_collection.delete_many(
                [
                    i.model_dump(by_alias=True, mode="json")
                    for i in links_to_delete
                ]
            )
        if links_to_create:
            self.tmo_edge_collection.insert_many(
                [
                    i.model_dump(by_alias=True, mode="json")
                    for i in links_to_create
                ]
            )
        if items_to_create:
            self._create(items=items_to_create)

    def _delete(self, items: list[TPRM]):
        tmo_ids = list(set(i.tmo_id for i in items))
        db_tmo_nodes = self.__get_db_tmo_nodes_dict(tmo_ids=tmo_ids)
        tmos_to_update = []
        tprm_id_to_delete_mo_link = []
        for item in items:
            tmo_node = db_tmo_nodes.get(item.tmo_id, None)
            if not tmo_node:
                continue
            # delete old param if exist
            tmo_node.params = [i for i in tmo_node.params if i.id != item.id]
            tmos_to_update.append(tmo_node)
            if item.val_type == "mo_link":
                tprm_id_to_delete_mo_link.append(item.id)
            # busy params
            if tmo_node.busy_parameter_groups:
                for group in tmo_node.busy_parameter_groups:
                    if item.id in group:
                        group.remove(item.id)
        if tmos_to_update:
            self.tmo_collection.update_many(
                [
                    i.model_dump(by_alias=True, mode="json")
                    for i in tmos_to_update
                ]
            )
        if tprm_id_to_delete_mo_link:
            self.delete_mo_links_by_tprm_ids(tprm_ids=tprm_id_to_delete_mo_link)

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

    @staticmethod
    def _create_mo_links(
        node: DbTmoNode,
        other_side_tmo_nodes: list[DbTmoNode],
        tprm: TPRM | None,
    ) -> list[TmoEdge]:
        results = []
        for other_side_tmo_node in other_side_tmo_nodes:
            result = TmoEdge(
                _from=node.id,
                _to=other_side_tmo_node.id,
                link_type=LinkType.MO_LINK,
                enabled=False,
                tprm_id=tprm.id if tprm else None,
            )
            results.append(result)
        return results

    def _delete_mo_links(
        self, node: DbTmoNode, other_side_tmo_nodes: list[DbTmoNode], tprm: TPRM
    ) -> list[DbTmoEdge]:
        query = """
            FOR edge IN @@tmoEdgeCollection
                FILTER edge._from == @nodeId
                FILTER edge._to IN @otherSideNodeIds
                FILTER edge.link_type == "point_tmo_constraint"
                FILTER edge.tprm_id == @tprm_id
                RETURN edge
        """
        binds = {
            "@tmoEdgeCollection": self.tmo_edge_collection.name,
            "nodeId": node.id,
            "otherSideNodeIds": [
                self.config.get_tmo_collection_key(i.tmo_id)
                for i in other_side_tmo_nodes
            ],
            "tprm_id": tprm.id,
        }
        response = self.database.aql.execute(query=query, bind_vars=binds)
        results = [DbTmoEdge.model_validate(i) for i in response]
        return results

    def delete_mo_links_by_tprm_ids(self, tprm_ids: list[int]):
        if not tprm_ids:
            return
        query = """
            FOR edge IN @@tmoEdgeCollection
                FILTER edge.tprm_id IN @tprmIds
                REMOVE edge IN @@tmoEdgeCollection
        """
        binds = {
            "@tmoEdgeCollection": self.tmo_edge_collection.name,
            "tprmIds": tprm_ids,
        }
        self.database.aql.execute(query=query, bind_vars=binds)

    def _create(self, items: list[TPRM]):
        tmo_ids = list(set(i.tmo_id for i in items))
        db_tmo_nodes = self.__get_db_tmo_nodes_dict(tmo_ids=tmo_ids)
        tmos_to_update = []
        new_links = []
        old_links = []
        for item in items:
            tmo_node = db_tmo_nodes.get(item.tmo_id, None)
            if not tmo_node:
                continue
            # delete old param if exist
            old_param: TPRM | None = None
            params = []
            for param in tmo_node.params:
                if param.id == item.id:
                    old_param = param
                else:
                    params.append(param)
            tmo_node.params = params

            if item.val_type == "mo_link":
                if not item.constraint:
                    item.constraint = json.dumps(
                        self.inventory.get_tprm_const(tprm_id=item.id)
                    )
                if item.constraint:
                    old_constraint = (
                        literal_eval(old_param.constraint)
                        if old_param
                        else None
                    )
                    if not old_constraint:
                        old_constraint = []
                    constraint = (
                        literal_eval(item.constraint)
                        if isinstance(item.constraint, str)
                        else item.constraint
                    )
                    if not constraint:
                        constraint = []
                    elif isinstance(constraint, Iterable):
                        constraint = list(constraint)
                    else:
                        constraint = [constraint]
                    new_constraint_ids = set(constraint).difference(
                        old_constraint
                    )
                    if new_constraint_ids:
                        other_side_tmo_nodes = self.__get_db_tmo_nodes_dict(
                            tmo_ids=list(new_constraint_ids)
                        )
                        mo_links = self._create_mo_links(
                            node=tmo_node,
                            other_side_tmo_nodes=list(
                                other_side_tmo_nodes.values()
                            ),
                            tprm=item,
                        )
                        new_links.extend(mo_links)
                    old_constraint_ids = set(old_constraint).difference(
                        constraint
                    )
                    if old_constraint_ids:
                        other_side_tmo_nodes = self.__get_db_tmo_nodes_dict(
                            tmo_ids=list(old_constraint_ids)
                        )
                        mo_links = self._delete_mo_links(
                            node=tmo_node,
                            other_side_tmo_nodes=list(
                                other_side_tmo_nodes.values()
                            ),
                            tprm=item,
                        )
                        old_links.extend(mo_links)

            tmo_node.params.append(item)
            tmos_to_update.append(tmo_node)
        if tmos_to_update:
            self.tmo_collection.update_many(
                [
                    i.model_dump(by_alias=True, mode="json")
                    for i in tmos_to_update
                ]
            )
        if new_links:
            self.tmo_edge_collection.insert_many(
                [i.model_dump(by_alias=True, mode="json") for i in new_links]
            )
        if old_links:
            self.tmo_edge_collection.delete_many(
                [i.model_dump(by_alias=True, mode="json") for i in old_links]
            )
