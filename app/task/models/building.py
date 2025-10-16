from __future__ import annotations

from pydantic import BaseModel, Field

from task.models.dto import DbMoNode, DbTmoNode
from task.models.enums import LinkType


class HierarchicalDbMo(DbMoNode):
    children: list[HierarchicalDbMo] = Field(default_factory=list)
    parent: HierarchicalDbMo | None = None
    links: set[int] = Field(default_factory=set)

    def get_all_links(self) -> set[int]:
        result = self.links.copy()
        for child in self.children:
            result.update(child.get_all_links())
        return result

    def contains_tmo_id(self, tmo_id: int) -> bool:
        if tmo_id == self.tmo_id:
            return True
        for child in self.children:
            if child.contains_tmo_id(tmo_id):
                return True
        else:
            return False

    def get_all_tmo_ids(self) -> list[int]:
        result: list[int] = [self.tmo_id]
        for child in self.children:
            result.extend(child.get_all_tmo_ids())
        return result

    def get_ids(self) -> list[int]:
        result: list[int] = [self.id]
        if self.parent:
            result.extend(self.parent.get_ids())
        return result

    def get_nearest_parent_id(self, other: HierarchicalDbMo) -> int | None:
        other_ids: set[int] = set(other.get_ids())
        for i in self.get_ids():
            if i in other_ids:
                return i


class HierarchicalDbTmo(DbTmoNode):
    children: list[HierarchicalDbTmo] = Field(default_factory=lambda: [])
    links: set[int] = Field(default_factory=lambda: set())

    def get_all_links(self) -> set[int]:
        result = self.links.copy()
        for child in self.children:
            result.update(child.get_all_links())
        return result

    def contains_tmo_id(self, tmo_id: int) -> bool:
        if tmo_id == self.tmo_id:
            return True
        for child in self.children:
            if child.contains_tmo_id(tmo_id):
                return True
        else:
            return False

    def get_all_tmo_ids(self) -> list[int]:
        result: list[int] = [self.tmo_id]
        for child in self.children:
            result.extend(child.get_all_tmo_ids())
        return result


class ConstraintFilter(BaseModel):
    link_type: LinkType
    tprm_id: int | None = None
    to_tmo_id: list[int]
