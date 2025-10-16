from datetime import datetime
import pickle
from typing import Any, Iterator, Literal

import dateutil.parser
from pydantic import Field, computed_field
from pydantic_settings import BaseSettings, SettingsConfigDict

from services.inventory import InventoryInterface

ARANGODB_IMAGE_NAME = "arangodb"
IMAGE_VERSION = "latest"
ARANGO_URL = f"{ARANGODB_IMAGE_NAME}:{IMAGE_VERSION}"


class ArangoTestConfig(BaseSettings):
    protocol: Literal["http", "https"] = Field("http", min_length=4)
    host: str = Field("localhost", min_length=1)
    port: int = Field(8529, ge=0)
    username: str = Field("root", min_length=1)
    password: str = Field("rootpassword", min_length=1)
    run_container_arango_local: bool = Field(
        default=False, alias="tests_run_container_arango_local"
    )

    @computed_field  # type: ignore
    @property
    def url(self) -> str:
        _url = f"{self.protocol}://{self.host}"
        if self.port is not None:
            _url = f"{_url}:{self.port}"
        return _url

    model_config = SettingsConfigDict(env_prefix="tests_arango_")


arango_test_config = ArangoTestConfig()


def arango_db_settings():
    settings = {
        "username": arango_test_config.username,
        "password": arango_test_config.password,
    }
    return settings


class Inventory(InventoryInterface):
    CONVERTER = {
        # "str": str,
        "int": int,
        "float": float,
        "mo_link": int,
        "two-way link": int,
        "datetime": lambda x: dateutil.parser.parse(x).isoformat(),
        "date": lambda x: datetime.strptime(x, "%Y-%m-%d").date().isoformat(),
        "bool": lambda x: True if x.lower() in ["true", "1"] else False,
        "prm_link": int,
        # "user_link": str,
        # "formula": str,
    }

    def __init__(self, grpc_url: str):
        self.channel = ""
        self.stub = ""

    def get_point_tmo_const(self, tmo_id: int) -> list[int]:
        pass

    def get_tprm_const(self, tprm_id: int) -> list[int]:
        pass

    def get_tprms_by_tprm_id(self, tprm_ids: list[int]) -> list[dict]:
        pass

    def get_tmo_tree(self, tmo_id: int | None) -> list[dict]:
        _tmos = []
        if tmo_id == 42588:
            _tmos = [
                {
                    "name": "Transport Network Small",
                    "icon": "AccessibleForward",
                    "description": "Test transport network with a small number of objects for"
                    " quick testing of functionality",
                    "virtual": True,
                    "global_uniqueness": True,
                    "child": [
                        {
                            "name": "Location Small",
                            "p_id": 42588,
                            "icon": "LocationOn",
                            "global_uniqueness": True,
                            "child": [
                                {
                                    "name": "Microwave Small",
                                    "p_id": 42589,
                                    "icon": "Waves",
                                    "global_uniqueness": True,
                                    "child": [
                                        {
                                            "name": "Microwave Port",
                                            "p_id": 42591,
                                            "icon": "ImportExport",
                                            "points_constraint_by_tmo": [42610],
                                            "id": 42610,
                                            "virtual": False,
                                            "global_uniqueness": False,
                                            "materialize": False,
                                            "child": [],
                                            "minimize": False,
                                        }
                                    ],
                                    "id": 42591,
                                    "virtual": False,
                                    "materialize": False,
                                    "points_constraint_by_tmo": [],
                                    "minimize": False,
                                },
                                {
                                    "name": "Fiber Cable Small",
                                    "p_id": 42589,
                                    "icon": "FiberSmartRecord",
                                    "global_uniqueness": True,
                                    "child": [
                                        {
                                            "name": "Fiber Cable Thread",
                                            "p_id": 42592,
                                            "icon": "ImportExport",
                                            "points_constraint_by_tmo": [42616],
                                            "id": 42611,
                                            "virtual": False,
                                            "global_uniqueness": False,
                                            "materialize": False,
                                            "child": [],
                                            "minimize": False,
                                        }
                                    ],
                                    "id": 42592,
                                    "virtual": False,
                                    "materialize": False,
                                    "points_constraint_by_tmo": [],
                                    "minimize": False,
                                },
                                {
                                    "name": "Swith",
                                    "p_id": 42589,
                                    "icon": "ToggleOff",
                                    "global_uniqueness": True,
                                    "child": [
                                        {
                                            "name": "Swith Port",
                                            "p_id": 42600,
                                            "icon": "ImportExport",
                                            "points_constraint_by_tmo": [42616],
                                            "id": 42617,
                                            "virtual": False,
                                            "global_uniqueness": False,
                                            "materialize": False,
                                            "child": [],
                                            "minimize": False,
                                        }
                                    ],
                                    "id": 42600,
                                    "virtual": False,
                                    "materialize": False,
                                    "points_constraint_by_tmo": [],
                                    "minimize": False,
                                },
                                {
                                    "name": "Optical Cross Small",
                                    "p_id": 42589,
                                    "icon": "HdrStrong",
                                    "global_uniqueness": True,
                                    "points_constraint_by_tmo": [42611],
                                    "child": [
                                        {
                                            "name": "Optical Cross Thread ",
                                            "p_id": 42599,
                                            "icon": "ImportExport",
                                            "points_constraint_by_tmo": [
                                                42611,
                                                42617,
                                            ],
                                            "id": 42616,
                                            "virtual": False,
                                            "global_uniqueness": False,
                                            "materialize": False,
                                            "child": [],
                                            "minimize": False,
                                        }
                                    ],
                                    "id": 42599,
                                    "virtual": False,
                                    "materialize": False,
                                    "minimize": False,
                                },
                                {
                                    "name": "Sites Small",
                                    "p_id": 42589,
                                    "icon": "CellTower",
                                    "global_uniqueness": True,
                                    "child": [
                                        {
                                            "name": "Site Port",
                                            "p_id": 42590,
                                            "icon": "ImportExport",
                                            "id": 42604,
                                            "virtual": False,
                                            "global_uniqueness": False,
                                            "materialize": False,
                                            "points_constraint_by_tmo": [],
                                            "child": [],
                                            "minimize": False,
                                        }
                                    ],
                                    "id": 42590,
                                    "virtual": False,
                                    "materialize": False,
                                    "points_constraint_by_tmo": [],
                                    "minimize": False,
                                },
                            ],
                            "id": 42589,
                            "virtual": False,
                            "materialize": False,
                            "points_constraint_by_tmo": [],
                            "minimize": False,
                        },
                        {
                            "name": "Service Small",
                            "p_id": 42588,
                            "global_uniqueness": True,
                            "id": 42622,
                            "virtual": False,
                            "materialize": False,
                            "points_constraint_by_tmo": [],
                            "child": [],
                            "minimize": False,
                        },
                    ],
                    "id": 42588,
                    "materialize": False,
                    "points_constraint_by_tmo": [],
                    "minimize": False,
                }
            ]

        if tmo_id == 1:
            _tmos = [
                {
                    "name": "Transport Network Small",
                    "icon": "AccessibleForward",
                    "description": "Test transport network with a small number of objects for"
                    " quick testing of functionality",
                    "virtual": True,
                    "global_uniqueness": True,
                    "child": [
                        {
                            "name": "Location Small",
                            "p_id": 1,
                            "icon": "LocationOn",
                            "global_uniqueness": True,
                            "child": [
                                {
                                    "name": "Microwave Small",
                                    "p_id": 2,
                                    "icon": "Waves",
                                    "global_uniqueness": True,
                                    "child": [
                                        {
                                            "name": "Microwave Port",
                                            "p_id": 3,
                                            "icon": "ImportExport",
                                            "points_constraint_by_tmo": [4],
                                            "id": 4,
                                            "virtual": False,
                                            "global_uniqueness": False,
                                            "materialize": False,
                                            "child": [],
                                            "minimize": False,
                                        }
                                    ],
                                    "id": 3,
                                    "virtual": False,
                                    "materialize": False,
                                    "points_constraint_by_tmo": [],
                                    "minimize": False,
                                },
                                {
                                    "name": "Fiber Cable Small",
                                    "p_id": 2,
                                    "icon": "FiberSmartRecord",
                                    "global_uniqueness": True,
                                    "child": [
                                        {
                                            "name": "Fiber Cable Thread",
                                            "p_id": 5,
                                            "icon": "ImportExport",
                                            "points_constraint_by_tmo": [6],
                                            "id": 7,
                                            "virtual": False,
                                            "global_uniqueness": False,
                                            "materialize": False,
                                            "child": [],
                                            "minimize": False,
                                        }
                                    ],
                                    "id": 5,
                                    "virtual": False,
                                    "materialize": False,
                                    "points_constraint_by_tmo": [],
                                    "minimize": False,
                                },
                                {
                                    "name": "Swith",
                                    "p_id": 2,
                                    "icon": "ToggleOff",
                                    "global_uniqueness": True,
                                    "child": [
                                        {
                                            "name": "Swith Port",
                                            "p_id": 8,
                                            "icon": "ImportExport",
                                            "points_constraint_by_tmo": [6],
                                            "id": 9,
                                            "virtual": False,
                                            "global_uniqueness": False,
                                            "materialize": False,
                                            "child": [],
                                            "minimize": False,
                                        }
                                    ],
                                    "id": 8,
                                    "virtual": False,
                                    "materialize": False,
                                    "points_constraint_by_tmo": [],
                                    "minimize": False,
                                },
                                {
                                    "name": "Optical Cross Small",
                                    "p_id": 2,
                                    "icon": "HdrStrong",
                                    "global_uniqueness": True,
                                    "points_constraint_by_tmo": [7],
                                    "child": [
                                        {
                                            "name": "Optical Cross Thread ",
                                            "p_id": 10,
                                            "icon": "ImportExport",
                                            "points_constraint_by_tmo": [7, 9],
                                            "id": 6,
                                            "virtual": False,
                                            "global_uniqueness": False,
                                            "materialize": False,
                                            "child": [],
                                            "minimize": False,
                                        }
                                    ],
                                    "id": 10,
                                    "virtual": False,
                                    "materialize": False,
                                    "minimize": False,
                                },
                                {
                                    "name": "Sites Small",
                                    "p_id": 2,
                                    "icon": "CellTower",
                                    "global_uniqueness": True,
                                    "child": [
                                        {
                                            "name": "Site Port",
                                            "p_id": 11,
                                            "icon": "ImportExport",
                                            "id": 12,
                                            "virtual": False,
                                            "global_uniqueness": False,
                                            "materialize": False,
                                            "points_constraint_by_tmo": [],
                                            "child": [],
                                            "minimize": False,
                                        }
                                    ],
                                    "id": 11,
                                    "virtual": False,
                                    "materialize": False,
                                    "points_constraint_by_tmo": [],
                                    "minimize": False,
                                },
                            ],
                            "id": 2,
                            "virtual": False,
                            "materialize": False,
                            "points_constraint_by_tmo": [],
                            "minimize": False,
                        },
                        {
                            "name": "Service Small",
                            "p_id": 1,
                            "global_uniqueness": True,
                            "id": 13,
                            "virtual": False,
                            "materialize": False,
                            "points_constraint_by_tmo": [],
                            "child": [],
                            "minimize": False,
                        },
                    ],
                    "id": 1,
                    "materialize": False,
                    "points_constraint_by_tmo": [],
                    "minimize": False,
                }
            ]

        return _tmos

    def get_tprms_by_tmo_id(self, tmo_ids: list[int]) -> list[dict]:
        data = [
            {
                "name": "Name",
                "val_type": "str",
                "required": True,
                "returnable": True,
                "tmo_id": 42589,
                "id": 125943,
                "multiple": False,
                "field_value": "",
            },
            {
                "name": "Latitude",
                "val_type": "float",
                "required": True,
                "returnable": True,
                "constraint": "-90:90",
                "tmo_id": 42589,
                "id": 125897,
                "multiple": False,
                "field_value": "",
            },
            {
                "name": "Longitude",
                "val_type": "float",
                "required": True,
                "returnable": True,
                "constraint": "-180:180",
                "tmo_id": 42589,
                "id": 125898,
                "multiple": False,
                "field_value": "",
            },
            {
                "name": "Name",
                "val_type": "str",
                "required": True,
                "returnable": True,
                "tmo_id": 42590,
                "id": 125914,
                "multiple": False,
                "field_value": "",
            },
            {
                "name": "Name",
                "val_type": "str",
                "required": True,
                "returnable": True,
                "tmo_id": 42591,
                "id": 125912,
                "multiple": False,
                "field_value": "",
            },
            {
                "name": "Name",
                "val_type": "str",
                "required": True,
                "returnable": True,
                "tmo_id": 42592,
                "id": 125911,
                "multiple": False,
                "field_value": "",
            },
            {
                "name": "Name",
                "val_type": "str",
                "required": True,
                "returnable": True,
                "tmo_id": 42599,
                "id": 125913,
                "multiple": False,
                "field_value": "",
            },
            {
                "name": "Name",
                "val_type": "str",
                "required": True,
                "returnable": True,
                "tmo_id": 42600,
                "id": 125944,
                "multiple": False,
                "field_value": "",
            },
            {
                "name": "Name",
                "val_type": "str",
                "required": True,
                "returnable": True,
                "tmo_id": 42604,
                "id": 125921,
                "multiple": False,
                "field_value": "",
            },
            {
                "name": "Trace",
                "val_type": "mo_link",
                "constraint": "[42622]",
                "tmo_id": 42604,
                "id": 125952,
                "multiple": False,
                "required": False,
                "returnable": False,
                "field_value": "",
            },
            {
                "name": "Trace",
                "val_type": "mo_link",
                "constraint": "[42622]",
                "tmo_id": 42610,
                "id": 125953,
                "multiple": False,
                "required": False,
                "returnable": False,
                "field_value": "",
            },
            {
                "name": "Name",
                "val_type": "str",
                "required": True,
                "returnable": True,
                "tmo_id": 42610,
                "id": 125933,
                "multiple": False,
                "field_value": "",
            },
            {
                "name": "Trace",
                "val_type": "mo_link",
                "constraint": "[42622]",
                "tmo_id": 42611,
                "id": 125954,
                "multiple": False,
                "required": False,
                "returnable": False,
                "field_value": "",
            },
            {
                "name": "Name",
                "val_type": "str",
                "required": True,
                "returnable": True,
                "tmo_id": 42611,
                "id": 125930,
                "multiple": False,
                "field_value": "",
            },
            {
                "name": "Trace",
                "val_type": "mo_link",
                "constraint": "[42622]",
                "tmo_id": 42616,
                "id": 125955,
                "multiple": False,
                "required": False,
                "returnable": False,
                "field_value": "",
            },
            {
                "name": "Name",
                "val_type": "str",
                "required": True,
                "returnable": True,
                "tmo_id": 42616,
                "id": 125941,
                "multiple": False,
                "field_value": "",
            },
            {
                "name": "Name",
                "val_type": "str",
                "required": True,
                "returnable": True,
                "tmo_id": 42617,
                "id": 125942,
                "multiple": False,
                "field_value": "",
            },
            {
                "name": "Trace",
                "val_type": "mo_link",
                "constraint": "[42622]",
                "tmo_id": 42617,
                "id": 125956,
                "multiple": False,
                "required": False,
                "returnable": False,
                "field_value": "",
            },
            {
                "name": "Name",
                "val_type": "str",
                "required": True,
                "returnable": True,
                "tmo_id": 42622,
                "id": 125951,
                "multiple": False,
                "field_value": "",
            },
        ]
        return data

    def get_mos_by_tmo_id(
        self,
        tmo_id: int,
        mo_filter_by: dict | None = None,
        prm_filter_by: dict | None = None,
        keep_mo_without_prm: bool = False,
        chunk_size: int = 50,
    ) -> Iterator[list[dict]]:
        data = {
            42589: [
                {
                    "tmo_id": 42589,
                    "id": 10817849,
                    "name": "Location 2",
                    "latitude": 1.0,
                    "longitude": 1.0,
                    "active": True,
                    "version": 1,
                    "params": [
                        {
                            "tprm_id": 125897,
                            "mo_id": 10817849,
                            "value": 1.0,
                            "id": 310602568,
                            "version": 1,
                        },
                        {
                            "tprm_id": 125898,
                            "mo_id": 10817849,
                            "value": 1.0,
                            "id": 310602569,
                            "version": 1,
                        },
                        {
                            "tprm_id": 125943,
                            "mo_id": 10817849,
                            "value": "Location 2",
                            "id": 310602570,
                            "version": 1,
                        },
                    ],
                    "pov": "",
                    "geometry": "",
                    "model": "",
                    "point_a_id": 0,
                    "point_b_id": 0,
                    "status": "",
                },
                {
                    "tmo_id": 42589,
                    "id": 10817851,
                    "name": "Location 4",
                    "latitude": 3.0,
                    "longitude": 3.0,
                    "active": True,
                    "version": 1,
                    "params": [
                        {
                            "tprm_id": 125897,
                            "mo_id": 10817851,
                            "value": 3.0,
                            "id": 310602574,
                            "version": 1,
                        },
                        {
                            "tprm_id": 125898,
                            "mo_id": 10817851,
                            "value": 3.0,
                            "id": 310602575,
                            "version": 1,
                        },
                        {
                            "tprm_id": 125943,
                            "mo_id": 10817851,
                            "value": "Location 4",
                            "id": 310602576,
                            "version": 2,
                        },
                    ],
                    "pov": "",
                    "geometry": "",
                    "model": "",
                    "point_a_id": 0,
                    "point_b_id": 0,
                    "status": "",
                },
                {
                    "tmo_id": 42589,
                    "id": 10817850,
                    "name": "Location 3",
                    "latitude": 2.0,
                    "longitude": 2.0,
                    "active": True,
                    "version": 1,
                    "params": [
                        {
                            "tprm_id": 125897,
                            "mo_id": 10817850,
                            "value": 2.0,
                            "id": 310602571,
                            "version": 1,
                        },
                        {
                            "tprm_id": 125898,
                            "mo_id": 10817850,
                            "value": 2.0,
                            "id": 310602572,
                            "version": 1,
                        },
                        {
                            "tprm_id": 125943,
                            "mo_id": 10817850,
                            "value": "Location 3",
                            "id": 310602573,
                            "version": 1,
                        },
                    ],
                    "pov": "",
                    "geometry": "",
                    "model": "",
                    "point_a_id": 0,
                    "point_b_id": 0,
                    "status": "",
                },
                {
                    "tmo_id": 42589,
                    "id": 10817848,
                    "name": "Location 1",
                    "active": True,
                    "version": 1,
                    "params": [
                        {
                            "tprm_id": 125897,
                            "mo_id": 10817848,
                            "value": 0.0,
                            "id": 310602565,
                            "version": 1,
                        },
                        {
                            "tprm_id": 125898,
                            "mo_id": 10817848,
                            "value": 0.0,
                            "id": 310602566,
                            "version": 1,
                        },
                        {
                            "tprm_id": 125943,
                            "mo_id": 10817848,
                            "value": "Location 1",
                            "id": 310602567,
                            "version": 1,
                        },
                    ],
                    "latitude": 0.0,
                    "longitude": 0.0,
                    "pov": "",
                    "geometry": "",
                    "model": "",
                    "point_a_id": 0,
                    "point_b_id": 0,
                    "status": "",
                },
            ],
            42591: [
                {
                    "tmo_id": 42591,
                    "p_id": 10817849,
                    "id": 10817854,
                    "name": "Microwave 2",
                    "active": True,
                    "version": 1,
                    "params": [
                        {
                            "tprm_id": 125912,
                            "mo_id": 10817854,
                            "value": "Microwave 2",
                            "id": 310602579,
                            "version": 1,
                        }
                    ],
                    "latitude": 0.0,
                    "longitude": 0.0,
                    "pov": "",
                    "geometry": "",
                    "model": "",
                    "point_a_id": 0,
                    "point_b_id": 0,
                    "status": "",
                },
                {
                    "tmo_id": 42591,
                    "p_id": 10817848,
                    "id": 10817853,
                    "name": "Microwave 1",
                    "active": True,
                    "version": 2,
                    "params": [
                        {
                            "tprm_id": 125912,
                            "mo_id": 10817853,
                            "value": "Microwave 1",
                            "id": 310602578,
                            "version": 1,
                        }
                    ],
                    "latitude": 0.0,
                    "longitude": 0.0,
                    "pov": "",
                    "geometry": "",
                    "model": "",
                    "point_a_id": 0,
                    "point_b_id": 0,
                    "status": "",
                },
            ],
            42610: [
                {
                    "tmo_id": 42610,
                    "p_id": 10817853,
                    "id": 11001167,
                    "name": "Microwave 1-Port 1",
                    "active": True,
                    "version": 1,
                    "params": [
                        {
                            "tprm_id": 125933,
                            "mo_id": 11001167,
                            "value": "Port 1",
                            "id": 313898777,
                            "version": 1,
                        }
                    ],
                    "latitude": 0.0,
                    "longitude": 0.0,
                    "pov": "",
                    "geometry": "",
                    "model": "",
                    "point_a_id": 0,
                    "point_b_id": 0,
                    "status": "",
                },
                {
                    "tmo_id": 42610,
                    "p_id": 10817853,
                    "id": 10818187,
                    "name": "Microwave 1-Port 2",
                    "active": True,
                    "point_a_id": 10818188,
                    "version": 2,
                    "params": [
                        {
                            "tprm_id": 125933,
                            "mo_id": 10818187,
                            "value": "Port 2",
                            "id": 310614064,
                            "version": 1,
                        },
                        {
                            "tprm_id": 125953,
                            "mo_id": 10818187,
                            "value": 10818360,
                            "id": 310625555,
                            "version": 1,
                        },
                    ],
                    "latitude": 0.0,
                    "longitude": 0.0,
                    "pov": "",
                    "geometry": "",
                    "model": "",
                    "point_b_id": 0,
                    "status": "",
                },
                {
                    "tmo_id": 42610,
                    "p_id": 10817854,
                    "id": 10818188,
                    "name": "Microwave 2-Port 2",
                    "active": True,
                    "point_a_id": 10818187,
                    "version": 2,
                    "params": [
                        {
                            "tprm_id": 125953,
                            "mo_id": 10818188,
                            "value": 10818360,
                            "id": 310625556,
                            "version": 1,
                        },
                        {
                            "tprm_id": 125933,
                            "mo_id": 10818188,
                            "value": "Port 2",
                            "id": 310614065,
                            "version": 1,
                        },
                    ],
                    "latitude": 0.0,
                    "longitude": 0.0,
                    "pov": "",
                    "geometry": "",
                    "model": "",
                    "point_b_id": 0,
                    "status": "",
                },
            ],
            42592: [
                {
                    "tmo_id": 42592,
                    "p_id": 10817849,
                    "id": 10817855,
                    "name": "Fiber Cable 1",
                    "active": True,
                    "version": 1,
                    "params": [
                        {
                            "tprm_id": 125911,
                            "mo_id": 10817855,
                            "value": "Fiber Cable 1",
                            "id": 310602580,
                            "version": 1,
                        }
                    ],
                    "latitude": 0.0,
                    "longitude": 0.0,
                    "pov": "",
                    "geometry": "",
                    "model": "",
                    "point_a_id": 0,
                    "point_b_id": 0,
                    "status": "",
                }
            ],
            42611: [
                {
                    "tmo_id": 42611,
                    "p_id": 10817855,
                    "id": 10818189,
                    "name": "Fiber Cable 1-Thread 2",
                    "active": True,
                    "point_a_id": 10818190,
                    "version": 3,
                    "params": [
                        {
                            "tprm_id": 125930,
                            "mo_id": 10818189,
                            "value": "Thread 2",
                            "id": 310614066,
                            "version": 1,
                        },
                        {
                            "tprm_id": 125954,
                            "mo_id": 10818189,
                            "value": 10818360,
                            "id": 310625554,
                            "version": 1,
                        },
                    ],
                    "latitude": 0.0,
                    "longitude": 0.0,
                    "pov": "",
                    "geometry": "",
                    "model": "",
                    "point_b_id": 0,
                    "status": "",
                }
            ],
            42600: [
                {
                    "tmo_id": 42600,
                    "p_id": 10817851,
                    "id": 10817857,
                    "name": "Switch 1",
                    "active": True,
                    "version": 1,
                    "params": [
                        {
                            "tprm_id": 125944,
                            "mo_id": 10817857,
                            "value": "Switch 1",
                            "id": 310602582,
                            "version": 1,
                        }
                    ],
                    "latitude": 0.0,
                    "longitude": 0.0,
                    "pov": "",
                    "geometry": "",
                    "model": "",
                    "point_a_id": 0,
                    "point_b_id": 0,
                    "status": "",
                }
            ],
            42617: [
                {
                    "tmo_id": 42617,
                    "p_id": 10817857,
                    "id": 10818356,
                    "name": "Switch 1-Port 2",
                    "active": True,
                    "point_a_id": 10818355,
                    "version": 2,
                    "params": [
                        {
                            "tprm_id": 125942,
                            "mo_id": 10818356,
                            "value": "Port 2",
                            "id": 310619809,
                            "version": 1,
                        },
                        {
                            "tprm_id": 125956,
                            "mo_id": 10818356,
                            "value": 10818360,
                            "id": 310625560,
                            "version": 1,
                        },
                    ],
                    "latitude": 0.0,
                    "longitude": 0.0,
                    "pov": "",
                    "geometry": "",
                    "model": "",
                    "point_b_id": 0,
                    "status": "",
                }
            ],
            42599: [
                {
                    "tmo_id": 42599,
                    "p_id": 10817850,
                    "id": 10817856,
                    "name": "Optical Cross 1",
                    "active": True,
                    "point_a_id": 10818189,
                    "version": 2,
                    "params": [
                        {
                            "tprm_id": 125913,
                            "mo_id": 10817856,
                            "value": "Optical Cross 1",
                            "id": 310602581,
                            "version": 1,
                        }
                    ],
                    "latitude": 0.0,
                    "longitude": 0.0,
                    "pov": "",
                    "geometry": "",
                    "model": "",
                    "point_b_id": 0,
                    "status": "",
                }
            ],
            42616: [
                {
                    "tmo_id": 42616,
                    "p_id": 10817856,
                    "id": 10818190,
                    "name": "Optical Cross 1-Thread 4",
                    "active": True,
                    "point_a_id": 10818189,
                    "version": 2,
                    "params": [
                        {
                            "tprm_id": 125941,
                            "mo_id": 10818190,
                            "value": "Thread 4",
                            "id": 310614067,
                            "version": 1,
                        },
                        {
                            "tprm_id": 125955,
                            "mo_id": 10818190,
                            "value": 10818360,
                            "id": 310625557,
                            "version": 1,
                        },
                    ],
                    "latitude": 0.0,
                    "longitude": 0.0,
                    "pov": "",
                    "geometry": "",
                    "model": "",
                    "point_b_id": 0,
                    "status": "",
                },
                {
                    "tmo_id": 42616,
                    "p_id": 10817856,
                    "id": 10818355,
                    "name": "Optical Cross 1-Thread 5",
                    "active": True,
                    "point_a_id": 10818356,
                    "version": 2,
                    "params": [
                        {
                            "tprm_id": 125941,
                            "mo_id": 10818355,
                            "value": "Thread 5",
                            "id": 310619808,
                            "version": 1,
                        },
                        {
                            "tprm_id": 125955,
                            "mo_id": 10818355,
                            "value": 10818360,
                            "id": 310625558,
                            "version": 1,
                        },
                    ],
                    "latitude": 0.0,
                    "longitude": 0.0,
                    "pov": "",
                    "geometry": "",
                    "model": "",
                    "point_b_id": 0,
                    "status": "",
                },
            ],
            42590: [
                {
                    "tmo_id": 42590,
                    "p_id": 10817848,
                    "id": 10817852,
                    "name": "Base station 1",
                    "active": True,
                    "version": 1,
                    "params": [
                        {
                            "tprm_id": 125914,
                            "mo_id": 10817852,
                            "value": "Base station 1",
                            "id": 310602577,
                            "version": 2,
                        }
                    ],
                    "latitude": 0.0,
                    "longitude": 0.0,
                    "pov": "",
                    "geometry": "",
                    "model": "",
                    "point_a_id": 0,
                    "point_b_id": 0,
                    "status": "",
                }
            ],
            42604: [
                {
                    "tmo_id": 42604,
                    "p_id": 10817852,
                    "id": 10818186,
                    "name": "Base station 1-Port 1",
                    "active": True,
                    "version": 1,
                    "params": [
                        {
                            "tprm_id": 125921,
                            "mo_id": 10818186,
                            "value": "Port 1",
                            "id": 310614063,
                            "version": 1,
                        },
                        {
                            "tprm_id": 125952,
                            "mo_id": 10818186,
                            "value": 10818360,
                            "id": 310625559,
                            "version": 1,
                        },
                    ],
                    "latitude": 0.0,
                    "longitude": 0.0,
                    "pov": "",
                    "geometry": "",
                    "model": "",
                    "point_a_id": 0,
                    "point_b_id": 0,
                    "status": "",
                },
                {
                    "tmo_id": 42604,
                    "id": 10838180,
                    "name": "Port 2 (test)",
                    "active": True,
                    "version": 1,
                    "params": [
                        {
                            "tprm_id": 125921,
                            "mo_id": 10838180,
                            "value": "Port 2 (test)",
                            "id": 311123470,
                            "version": 1,
                        },
                        {
                            "tprm_id": 125952,
                            "mo_id": 10838180,
                            "value": 10818360,
                            "id": 311123471,
                            "version": 1,
                        },
                    ],
                    "latitude": 0.0,
                    "longitude": 0.0,
                    "pov": "",
                    "geometry": "",
                    "model": "",
                    "point_a_id": 0,
                    "point_b_id": 0,
                    "status": "",
                },
                {
                    "tmo_id": 42604,
                    "id": 10840278,
                    "name": "Port 3 (test)",
                    "active": True,
                    "version": 1,
                    "params": [
                        {
                            "tprm_id": 125921,
                            "mo_id": 10840278,
                            "value": "Port 3 (test)",
                            "id": 311136048,
                            "version": 1,
                        },
                        {
                            "tprm_id": 125952,
                            "mo_id": 10840278,
                            "value": 10818360,
                            "id": 311136049,
                            "version": 1,
                        },
                    ],
                    "latitude": 0.0,
                    "longitude": 0.0,
                    "pov": "",
                    "geometry": "",
                    "model": "",
                    "point_a_id": 0,
                    "point_b_id": 0,
                    "status": "",
                },
            ],
            42622: [
                {
                    "tmo_id": 42622,
                    "id": 10818360,
                    "name": "4G-123",
                    "active": True,
                    "version": 1,
                    "params": [
                        {
                            "tprm_id": 125951,
                            "mo_id": 10818360,
                            "value": "4G-123",
                            "id": 310619813,
                            "version": 1,
                        }
                    ],
                    "latitude": 0.0,
                    "longitude": 0.0,
                    "pov": "",
                    "geometry": "",
                    "model": "",
                    "point_a_id": 0,
                    "point_b_id": 0,
                    "status": "",
                }
            ],
        }
        if tmo_id in data:
            yield data.get(tmo_id)
        else:
            return

    def _convert_prm_val_type(
        self, prms: list[dict], tprms: dict[int, dict]
    ) -> list[dict]:
        converted_prms = []
        for prm in prms:
            prm["tprm_id"] = int(prm["tprm_id"])
            prm["mo_id"] = int(prm["mo_id"])
            prm["id"] = int(prm["id"])
            tprm = tprms.get(prm["tprm_id"])
            if not tprm:
                raise ValueError(
                    "Tprm id {} not found in tprms. PRM id is {}".format(
                        prm["tprm_id"], prm["id"]
                    )
                )
            if tprm["multiple"]:
                prm["value"] = pickle.loads(bytes.fromhex(prm["value"]))
            elif tprm["val_type"] in self.CONVERTER:
                prm["value"] = self.CONVERTER[tprm["val_type"]](prm["value"])
            converted_prms.append(prm)
        return converted_prms

    def _convert_mo(
        self, mos: list[dict], tprms: dict[int, dict]
    ) -> list[dict]:
        converted_mos = []
        for mo in mos:
            mo = mo.copy()
            mo["params"] = self._convert_prm_val_type(
                prms=mo["params"], tprms=tprms
            )
            mo["tmo_id"] = int(mo["tmo_id"])
            if "p_id" in mo:
                mo["p_id"] = int(mo["p_id"])
            mo["id"] = int(mo["id"])
            mo["point_a_id"] = int(mo["point_a_id"])
            mo["point_b_id"] = int(mo["point_b_id"])
            mo["version"] = int(mo["version"])
            converted_mos.append(mo)
        return converted_mos

    def get_tmo_by_mo_id(self, mo_id: int) -> int | None:
        if mo_id == 10818360:
            return 42622
        else:
            return

    def get_mos_by_mo_ids(self, mo_ids: list[int]) -> list[dict]:
        data = {
            10818360: {
                "tmo_id": 42622,
                "id": 10818360,
                "name": "4G-123",
                "active": True,
                "version": 1,
                "latitude": 0.0,
                "longitude": 0.0,
                "pov": "",
                "geometry": "",
                "model": "",
                "point_a_id": 0,
                "point_b_id": 0,
                "status": "",
                "params": [],
            }
        }
        result = []
        for mo_id in mo_ids:
            if mo_id not in data:
                continue
            result.append(data[mo_id])
        return result

    def get_prms_by_prm_ids(self, prm_ids: list[int]) -> list[dict]:
        pass


class MOs:
    @staticmethod
    def get_mos(row: str = None, value: Any = None):
        mos = [
            {
                "tmo_id": 1,
                "p_id": None,
                "id": 1,
                "name": "1",
                "latitude": None,
                "longitude": None,
                "pov": {},
                "geometry": {},
                "model": None,
                "active": True,
                "point_a_id": None,
                "point_b_id": None,
                "status": None,
                "version": 1,
                "params": [
                    {
                        "tprm_id": 1,
                        "mo_id": 1,
                        "value": "first_val",
                        "id": 1,
                        "version": 1,
                    }
                ],
            }
        ]
        if row is not None and value is not None:
            result = []
            for mo in mos:
                if mo[row] == value:
                    result.append(mo)
        return mos


class TRPMs:
    @staticmethod
    def get_tprms(row: str = None, value: Any = None):
        tprms = [
            {
                "tmo_id": 1,
                "name": "tprm_of_parent_tmo_1",
                "val_type": "str",
                "multiple": False,
                "required": False,
                "constraint": None,
                "returnable": False,
                "description": None,
                "prm_link_filter": None,
                "group": None,
                "id": 1,
            },
            {
                "tmo_id": 2,
                "name": "tprm_of_parent_tmo_2",
                "val_type": "str",
                "multiple": False,
                "required": False,
                "constraint": None,
                "returnable": False,
                "description": "",
                "prm_link_filter": None,
                "group": None,
                "id": 2,
            },
        ]
        if row is not None and value is not None:
            result = []
            for tprm in tprms:
                if tprm[row] == value:
                    result.append(tprm)
            return result
        return tprms


class TMOs:
    _tmos = [
        {
            "name": "parent_tmo",
            "p_id": None,
            "icon": None,
            "description": None,
            "virtual": False,
            "global_uniqueness": False,
            "lifecycle_process_definition": None,
            "geometry_type": None,
            "materialize": False,
            "points_constraint_by_tmo": [],
            "id": 1,
        },
        {
            "name": "child_tmo",
            "p_id": 1,
            "icon": None,
            "description": None,
            "virtual": False,
            "global_uniqueness": False,
            "lifecycle_process_definition": None,
            "geometry_type": None,
            "materialize": False,
            "points_constraint_by_tmo": [],
            "id": 2,
        },
        {
            "name": "child_tmo1",
            "p_id": 2,
            "icon": None,
            "description": None,
            "virtual": False,
            "global_uniqueness": False,
            "lifecycle_process_definition": None,
            "geometry_type": None,
            "materialize": False,
            "points_constraint_by_tmo": [],
            "id": 3,
        },
    ]

    @classmethod
    def get_tmos(cls, row: str = None, value: Any = None):
        if row is not None and value is not None:
            result = []
            for tmo in cls._tmos:
                if tmo[row] == value:
                    result.append(tmo)
            return result
        return cls._tmos
