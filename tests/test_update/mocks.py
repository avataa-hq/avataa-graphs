from datetime import datetime
from typing import Iterator

from services.inventory import InventoryInterface


class InventoryMock(InventoryInterface):
    def __init__(self):
        self.point_tmo_const = {100500: [44483]}
        self.tmo_tree = {
            1: [
                {
                    "name": "Test TMO",
                    "p_id": 44483,
                    "icon": None,
                    "description": None,
                    "virtual": False,
                    "global_uniqueness": True,
                    "lifecycle_process_definition": None,
                    "geometry_type": None,
                    "materialize": False,
                    "point_tmo_const": [],
                    "child": [
                        {
                            "name": "Test TMO child",
                            "p_id": 1,
                            "icon": None,
                            "description": None,
                            "virtual": False,
                            "global_uniqueness": True,
                            "lifecycle_process_definition": None,
                            "geometry_type": None,
                            "materialize": False,
                            "point_tmo_const": [1, 44483, 100500],
                            "child": [],
                            "id": 100500,
                            "minimize": False,
                            "created_by": "",
                            "modified_by": "",
                            "latitude": None,
                            "longitude": None,
                            "creation_date": datetime(2020, 1, 1),
                            "modification_date": datetime(2020, 1, 1),
                            "primary": [],
                            "severity_id": None,
                            "status": None,
                            "version": 1,
                            "line_type": None,
                        }
                    ],
                    "id": 1,
                    "minimize": False,
                    "created_by": "",
                    "modified_by": "",
                    "latitude": None,
                    "longitude": None,
                    "creation_date": datetime(2020, 1, 1),
                    "modification_date": datetime(2020, 1, 1),
                    "primary": [],
                    "severity_id": None,
                    "status": None,
                    "version": 1,
                    "line_type": None,
                }
            ]
        }
        self.mos = {
            100500: {
                "active": True,
                "geometry": None,
                "id": 100500,
                "latitude": 40.750008,
                "longitude": -73.98783,
                "model": None,
                "name": "Cluster Test",
                "p_id": 11218680,
                "params": [],
                "point_a_id": None,
                "point_b_id": None,
                "pov": None,
                "status": None,
                "tmo_id": 43622,
                "version": 2,
            },
            11218680: {
                "active": True,
                "geometry": None,
                "id": 11218680,
                "latitude": None,
                "longitude": None,
                "model": None,
                "name": "Region 1",
                "p_id": None,
                "params": [
                    {
                        "tprm_id": 130060,
                        "mo_id": 11218680,
                        "value": "Region 1",
                        "id": 320151946,
                        "version": 1,
                    }
                ],
                "point_a_id": None,
                "point_b_id": None,
                "pov": None,
                "status": None,
                "tmo_id": 43623,
                "version": 1,
            },
            11237158: {
                "active": True,
                "geometry": None,
                "id": 11237158,
                "latitude": None,
                "longitude": None,
                "model": None,
                "name": "Test Data Inventory",
                "p_id": None,
                "params": [],
                "point_a_id": None,
                "point_b_id": None,
                "pov": None,
                "status": None,
                "tmo_id": 43623,
                "version": 1,
            },
            11237488: {
                "active": True,
                "geometry": None,
                "id": 11237488,
                "latitude": None,
                "longitude": None,
                "model": None,
                "name": "TN S24",
                "p_id": None,
                "params": [
                    {
                        "tprm_id": 130068,
                        "mo_id": 11237488,
                        "value": "TN S24",
                        "id": 320958600,
                        "version": 3,
                        "parsed_value": None,
                    },
                    {
                        "tprm_id": 131818,
                        "mo_id": 11237488,
                        "value": "TN S24",
                        "id": 320958677,
                        "version": 3,
                        "parsed_value": None,
                    },
                ],
                "point_a_id": None,
                "point_b_id": None,
                "pov": None,
                "status": None,
                "tmo_id": 43626,
                "version": 1,
            },
            11218681: {
                "active": True,
                "geometry": None,
                "id": 11218681,
                "latitude": None,
                "longitude": None,
                "model": None,
                "name": "Region 2",
                "p_id": None,
                "params": [
                    {
                        "tprm_id": 130060,
                        "mo_id": 11218681,
                        "value": "Region 2",
                        "id": 320151947,
                        "version": 1,
                        "parsed_value": None,
                    }
                ],
                "point_a_id": None,
                "point_b_id": None,
                "pov": None,
                "status": None,
                "tmo_id": 43623,
                "version": 1,
            },
            11237227: {
                "active": True,
                "geometry": None,
                "id": 11237227,
                "latitude": None,
                "longitude": None,
                "model": None,
                "name": "TN S1 ",
                "p_id": None,
                "params": [
                    {
                        "tprm_id": 130068,
                        "mo_id": 11237227,
                        "value": "TN S1 ",
                        "id": 320958079,
                        "version": 5,
                        "parsed_value": None,
                    },
                    {
                        "tprm_id": 131818,
                        "mo_id": 11237227,
                        "value": "TN S1 ",
                        "id": 320958676,
                        "version": 2,
                        "parsed_value": None,
                    },
                ],
                "point_a_id": None,
                "point_b_id": None,
                "pov": None,
                "status": None,
                "tmo_id": 43626,
                "version": 1,
            },
        }

    def get_tmo_tree(self, tmo_id: int | None) -> list[dict]:
        print("get_tmo_tree", tmo_id)
        return self.tmo_tree.get(tmo_id, [])

    def get_tprms_by_tmo_id(self, tmo_ids: list[int]) -> list[dict]:
        pass

    def get_mos_by_tmo_id(
        self,
        tmo_id: int,
        mo_filter_by: dict | None = None,
        prm_filter_by: dict | None = None,
        keep_mo_without_prm: bool = False,
        chunk_size: int = 50,
    ) -> Iterator[list[dict]]:
        pass

    def get_tmo_by_mo_id(self, mo_id: int) -> int:
        print("get_tmo_by_mo_id", f"{mo_id=}")
        pass

    def get_mos_by_mo_ids(self, mo_ids: list[int]) -> list[dict]:
        print("get_mos_by_mo_ids", f"{mo_ids=}")
        results = [self.mos[i] for i in mo_ids if i in self.mos]
        return results

    def get_prms_by_prm_ids(self, prm_ids: list[int]) -> list[dict]:
        print("get_prms_by_prm_ids", f"{prm_ids=}")
        pass

    def get_tprm_const(self, tprm_id: int) -> list[int]:
        print("get_tprm_const", f"{tprm_id=}")
        pass

    def get_point_tmo_const(self, tmo_id: int) -> list[int]:
        return self.point_tmo_const.get(tmo_id, [])

    def get_tprms_by_tprm_id(self, tprm_ids: list[int]) -> list[dict]:
        print("get_tprms_by_tprm_id", f"{tprm_ids=}")
        pass
