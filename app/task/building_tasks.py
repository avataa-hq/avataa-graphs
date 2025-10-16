from datetime import datetime

from arango.exceptions import AQLQueryExecuteError

from services.graph import GraphService, IfNotExistType
from services.inventory import InventoryInterface
from task.building_helpers.add_breadcrumbs import add_breadcrumbs
from task.building_helpers.build_from_tmo import build_from_tmo
from task.building_helpers.build_links_from_tmo import build_links_from_tmo
from task.building_helpers.connect_service_by_lines import (
    connect_service_by_lines,
)
from task.building_helpers.fill_path_edge_collection import (
    fill_path_edge_collection,
)
from task.building_helpers.forward_line_connections import (
    forward_line_connections,
)
from task.building_helpers.forward_service_connections_by_mo_links import (
    forward_service_connections_by_mo_links,
)
from task.building_helpers.group_nodes import group_nodes
from task.building_helpers.spread_connections import spread_connections
from task.models.building import HierarchicalDbTmo
from task.models.dto import DbTmoNode
from task.models.enums import Status
from task.models.errors import GraphBuildingError, TraceNodeNotFound
from task.task_abstract import TaskAbstract, TaskChecks


class DeleteOrhanBranchesSubtask(TaskAbstract, TaskChecks):
    def __init__(
        self,
        graph_db: GraphService,
        key: str,
    ):
        TaskAbstract.__init__(self, graph_db=graph_db, key=key)

    def create_hierarchical_tmo_tree(self) -> list[HierarchicalDbTmo]:
        top_level: list[HierarchicalDbTmo] = []
        levels: dict[str, HierarchicalDbTmo] = {}

        query = """
            FOR v, e IN 1..1 INBOUND @tmo GRAPH @tmoGraph
                FILTER e.link_type == 'p_id'
                RETURN v
        """
        binds = {"tmoGraph": self.config.tmo_graph_name}

        start_tmo = self.config.get_tmo_collection_key(self.document.tmo_id)
        queue = [start_tmo]
        is_top_level = True
        while queue:
            current_tmo = queue.pop(0)
            binds["tmo"] = current_tmo
            for child_tmo_data in self.database.aql.execute(
                query=query, bind_vars=binds
            ):
                db_tmo = HierarchicalDbTmo.model_validate(child_tmo_data)
                queue.append(db_tmo.id)
                levels[db_tmo.id] = db_tmo
                if not is_top_level:
                    current_db_tmo = levels[current_tmo]
                    current_db_tmo.children.append(db_tmo)
            if is_top_level:
                top_level = list(levels.values())
                is_top_level = False
        return top_level

    def get_cross_links(
        self, tree: list[HierarchicalDbTmo]
    ) -> list[HierarchicalDbTmo]:
        query = """
            LET nodeIds = (
                FOR doc IN @@mainCollection
                    FILTER doc.tmo == @tmoId
                    RETURN doc._id
                )

            LET toIds = (FOR edge IN @@mainEdgesCollection
                FILTER edge.connection_type != "p_id"
                FILTER edge._from IN nodeIds
                RETURN edge._to)

            FOR doc IN @@mainCollection
                FILTER doc._id IN toIds
                COLLECT tmos = doc.tmo
                RETURN tmos
        """
        binds = {
            "@mainCollection": self.config.graph_data_collection_name,
            "@mainEdgesCollection": self.config.graph_data_edge_name,
        }

        queue: list[HierarchicalDbTmo] = [*tree]
        while queue:
            db_tmo: HierarchicalDbTmo = queue.pop(0)
            binds["tmoId"] = db_tmo.tmo_id
            connected_with = self.database.aql.execute(
                query=query, bind_vars=binds
            )
            db_tmo.links.update(connected_with)
            queue.extend(db_tmo.children)

        return tree

    def find_unrelated_tmos(
        self, tree: list[HierarchicalDbTmo]
    ) -> list[HierarchicalDbTmo]:
        start_tmo = self.document.tmo_id
        # replace if installed another
        start_from = self.config_collection.get({"_key": "start_from"})
        if start_from:
            start_from = start_from.get("tmo_id")
            if start_from:
                start_tmo = start_from
        # find start node
        start_node = None
        for i in tree:
            if i.contains_tmo_id(start_tmo):
                start_node = i
                break
        if not start_node:
            return []

        cross_links: dict[int, set[int]] = {i.tmo_id: set() for i in tree}
        for i in tree:
            for link in i.get_all_links():
                if link not in cross_links:
                    continue
                cross_links[link].add(i.tmo_id)
                cross_links[i.tmo_id].add(link)
        has_link = {i: False for i in cross_links}
        queue: list[int] = [start_node.tmo_id]
        while queue:
            current_tmo_id = queue.pop(0)
            if has_link[current_tmo_id]:
                continue
            has_link[current_tmo_id] = True
            for link in cross_links[current_tmo_id]:
                if not has_link[link]:
                    queue.append(link)

        no_links_tmo_id: set[int] = {k for k, v in has_link.items() if not v}
        if not no_links_tmo_id:
            return []
        result = []
        for branch in tree:
            if branch.tmo_id in no_links_tmo_id:
                result.append(branch)
        return result

    def delete_orphan_branches(self, tree: list[HierarchicalDbTmo]):
        if not tree:
            return
        query = """
            LET nodeIds = (FOR doc IN @@mainCollection
                FILTER doc.tmo_id IN @tmoIds
                RETURN doc._id)

            FOR nodeId IN nodeIds
                REMOVE {"_id": nodeId} IN @@mainCollection

            FOR edge IN @@mainEdge
                FILTER edge._from IN nodeIds OR edge._to IN nodeIds
                REMOVE {"_key": edge._key} IN @@mainEdge
        """
        tmo_ids = [
            tmo_id for branch in tree for tmo_id in branch.get_all_tmo_ids()
        ]
        if not tmo_ids:
            return
        binds = {
            "@mainCollection": self.config.graph_data_collection_name,
            "@mainEdge": self.config.graph_data_edge_name,
            "tmoIds": tmo_ids,
        }
        try:
            self.database.aql.execute(query=query, bind_vars=binds)
        except AQLQueryExecuteError as ex:
            print(ex)
            print(query)
            print(1)
        except Exception as ex:
            print(ex)
            print(type(ex))
            print(1)

    def execute(self):
        tree = self.create_hierarchical_tmo_tree()
        tree = self.get_cross_links(tree)
        tree = self.find_unrelated_tmos(tree=tree)
        self.delete_orphan_branches(tree=tree)


class RunBuildingTask(TaskAbstract, TaskChecks):
    def __init__(
        self,
        graph_db: GraphService,
        inventory: InventoryInterface,
        key: str,
    ):
        TaskAbstract.__init__(self, graph_db=graph_db, key=key)
        self.inventory = inventory

    def check(self):
        self.check_status(
            document=self.document, impossible_status=[Status.IN_PROCESS]
        )
        self.check_collection(
            document=self.document, tmo_collection=self.tmo_collection
        )

    def update_document(self):
        self.system_main_collection.insert(
            self.document.model_dump(mode="json", by_alias=True),
            overwrite=True,
            overwrite_mode="replace",
            keep_none=True,
        )
        # Let's reset. To request the document again at the next step
        self._document = None

    def build_as_in_inventory(self):
        start_from_tmo = self.tmo_collection.get(
            document=str(self.document.tmo_id)
        )
        start_from_tmo = DbTmoNode.model_validate(start_from_tmo)
        build_from_tmo(
            tmo_node=start_from_tmo,
            is_trace=False,
            inventory=self.inventory,
            task=self,
            tmo_edge=None,
        )

    def build_trace_as_in_inventory(self):
        if not self.trace_tmo_id:
            return
        start_from_tmo = self.tmo_collection.get(
            document=str(self.trace_tmo_id)
        )
        if not start_from_tmo:
            raise TraceNodeNotFound(
                f"Node with tmo id {self.trace_tmo_id} not found"
            )
        start_from_tmo = DbTmoNode.model_validate(start_from_tmo)
        build_from_tmo(
            tmo_node=start_from_tmo,
            is_trace=True,
            inventory=self.inventory,
            task=self,
            tmo_edge=None,
        )

    def create_links(self):
        start_from_tmo = self.tmo_collection.get(
            document=str(self.document.tmo_id)
        )
        start_from_tmo = DbTmoNode.model_validate(start_from_tmo)
        build_links_from_tmo(tmo=start_from_tmo, task=self)

    def execute(self):
        print(f"Process {self.key} started")
        # COLLECTIONS CREATION
        self.graph_db.create_graph(
            db=self.database,
            name=self.config.graph_data_graph_name,
            edge_collection=self.config.graph_data_edge_name,
            from_vertex_collections=[self.config.graph_data_collection_name],
            to_vertex_collections=[self.config.graph_data_collection_name],
            if_exist=IfNotExistType.RETURN_NONE,
        )
        # Clearing the previous state
        self.main_collection.truncate()
        self.main_edge_collection.truncate()
        self.main_path_collection.truncate()
        try:
            # Status before
            self.document.status = Status.IN_PROCESS
            self.document.error_description = None
            self.update_document()

            # Main code. Attention: The order of execution is very important
            self.build_as_in_inventory()
            self.build_trace_as_in_inventory()
            self.create_links()
            fill_path_edge_collection(task=self)
            forward_service_connections_by_mo_links(task=self)
            group_nodes(task=self, inventory=self.inventory)
            forward_line_connections(task=self)
            spread_connections(task=self)
            connect_service_by_lines(task=self)
            add_breadcrumbs(task=self)

            if self.delete_orphan_branches_status:
                delete_task = DeleteOrhanBranchesSubtask(
                    graph_db=self.graph_db, key=self.key
                )
                delete_task.execute()

            # Status after
            self.document.status = Status.COMPLETE
            self.document.error_description = None
            self.update_document()
        except Exception as e:
            # Status error
            self.document.status = Status.ERROR
            self.document.error_description = str(e)
            self.update_document()

            raise GraphBuildingError(
                f"Error when building a graph with key {self.key}"
            ) from e
        else:
            print(f"{datetime.now()} Process {self.key} finished")
