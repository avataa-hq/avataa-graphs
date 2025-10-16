import abc
from abc import ABC
from datetime import datetime
import json
from multiprocessing import Lock
import pickle
from sys import stderr
import traceback
from typing import Iterator, Optional

import dateutil.parser
from google.protobuf.json_format import MessageToDict
import grpc
from grpc.aio import AioRpcError

from services.inventory_proto.graph_pb2 import (
    InMOsByMoIds,
    InMOsByTMOid,
    InPRMsByPRMIds,
    InTmoByMoId,
    InTmoId,
    InTmoIds,
    InTprmId,
    InTprmIds,
    OutMOsByMoIds,
    OutPRMsByPRMIds,
    OutTmoId,
    OutTmoIds,
    OutTprms,
)
from services.inventory_proto.graph_pb2_grpc import GraphInformerStub


class MockLock:
    def __enter__(self):
        pass

    def __exit__(self, exc_type, exc_val, exc_tb):
        pass


class InventoryInterface(ABC):
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

    @abc.abstractmethod
    def get_tmo_tree(self, tmo_id: int | None) -> list[dict]:
        pass

    @abc.abstractmethod
    def get_tprms_by_tmo_id(self, tmo_ids: list[int]) -> list[dict]:
        pass

    @abc.abstractmethod
    def get_mos_by_tmo_id(
        self,
        tmo_id: int,
        mo_filter_by: dict | None = None,
        prm_filter_by: dict | None = None,
        keep_mo_without_prm: bool = False,
        chunk_size: int = 50,
    ) -> Iterator[list[dict]]:
        pass

    @abc.abstractmethod
    def get_tmo_by_mo_id(self, mo_id: int) -> int:
        pass

    @abc.abstractmethod
    def get_mos_by_mo_ids(self, mo_ids: list[int]) -> list[dict]:
        pass

    @abc.abstractmethod
    def get_prms_by_prm_ids(self, prm_ids: list[int]) -> list[dict]:
        pass

    @abc.abstractmethod
    def get_point_tmo_const(self, tmo_id: int) -> list[int]:
        pass

    @abc.abstractmethod
    def get_tprm_const(self, tprm_id: int) -> list[int]:
        pass

    @abc.abstractmethod
    def get_tprms_by_tprm_id(self, tprm_ids: list[int]) -> list[dict]:
        pass


class Inventory(InventoryInterface):
    def __init__(self, grpc_url: str, lock: Optional[Lock] = None):
        self.grpc_url = grpc_url
        channel_options = [
            ("grpc.keepalive_time_ms", 30_000),
            ("grpc.keepalive_timeout_ms", 15_000),
            ("grpc.http2.max_pings_without_data", 5),
            ("grpc.keepalive_permit_without_calls", 1),
        ]
        service_config_json = json.dumps(
            {
                "methodConfig": [
                    {
                        "name": [{}],
                        "retryPolicy": {
                            "maxAttempts": 5,
                            "initialBackoff": "2s",
                            "maxBackoff": "15s",
                            "backoffMultiplier": 2,
                            "retryableStatusCodes": ["UNAVAILABLE"],
                        },
                    }
                ]
            }
        )
        channel_options.append(("grpc.service_config", service_config_json))
        self.channel = grpc.insecure_channel(
            target=grpc_url, options=channel_options
        )
        self.stub = GraphInformerStub(channel=self.channel)
        self.lock = lock or Lock()

    def get_tmo_tree(self, tmo_id: int | None) -> list[dict]:
        print("GRPC: get tmo tree")

        def tmp_int64_to_int(_node: dict) -> None:
            _node["id"] = int(_node["id"])
            if "p_id" in _node:
                _node["p_id"] = int(_node["p_id"])
            _node["points_constraint_by_tmo"] = [
                int(i) for i in _node["points_constraint_by_tmo"]
            ]
            _node["latitude"] = int(_node.get("latitude", 0))
            _node["longitude"] = int(_node.get("longitude", 0))
            _node["primary"] = [int(i) for i in _node["primary"]]
            _node["label"] = [int(i) for i in _node["label"]]
            _node["severity_id"] = int(_node.get("severity_id", 0))
            _node["status"] = int(_node.get("status", 0))
            for _child in _node["child"]:
                tmp_int64_to_int(_child)

        try:
            query = InTmoId(tmo_id=tmo_id)
            with self.lock:
                response = self.stub.GetTMOTree(query)
            nodes = MessageToDict(
                response,
                always_print_fields_with_no_presence=True,
                preserving_proto_field_name=True,
            )["nodes"]
            for node in nodes:
                tmp_int64_to_int(node)
            return nodes
        except AioRpcError:
            print(traceback.format_exc(), file=stderr)
            raise ValueError("Service error")

    def get_tprms_by_tmo_id(self, tmo_ids: list[int]) -> list[dict]:
        print("GRPC: get tprms by tmo id")
        if len(tmo_ids) <= 0:
            raise ValueError(f"Incorrect list of {tmo_ids=}")
        try:
            query = InTmoIds(tmo_id=tmo_ids)
            with self.lock:
                response = self.stub.GetTPRMsByTMOid(query, wait_for_ready=True)
            tprms = MessageToDict(
                response,
                always_print_fields_with_no_presence=True,
                preserving_proto_field_name=True,
            )["tprms"]
            for tprm in tprms:
                tprm["id"] = int(tprm["id"])
                tprm["tmo_id"] = int(tprm["tmo_id"])
            return tprms
        except AioRpcError:
            print(traceback.format_exc(), file=stderr)
            raise ValueError("Service error")

    def get_mos_by_tmo_id(
        self,
        tmo_id: int,
        mo_filter_by: dict | None = None,
        prm_filter_by: dict | None = None,
        keep_mo_without_prm: bool = False,
        chunk_size: int = 50,
    ) -> Iterator[list[dict]]:
        print("GRPC: get mo by tmo id")
        if tmo_id <= 0:
            raise ValueError(f"Incorrect value of {tmo_id=}")
        if chunk_size <= 0:
            raise ValueError(f"Incorrect value of {chunk_size=}")
        try:
            query = InMOsByTMOid(
                tmo_id=tmo_id,
                mo_filter_by=json.dumps(mo_filter_by) if mo_filter_by else None,
                prm_filter_by=json.dumps(prm_filter_by)
                if prm_filter_by
                else None,
                keep_mo_without_prm=keep_mo_without_prm,
                chunk_size=chunk_size,
            )

            tprms = self.get_tprms_by_tmo_id(tmo_ids=[tmo_id])
            tprms_dict = {i["id"]: i for i in tprms}
            # with self.lock:

            with grpc.insecure_channel(self.grpc_url) as channel:
                stub = GraphInformerStub(channel=channel)
                for chunk in stub.GetMOsByTMOid(query):
                    chunk_converted = self._convert_mo(
                        mos=MessageToDict(
                            chunk,
                            always_print_fields_with_no_presence=True,
                            preserving_proto_field_name=True,
                        )["mo"],
                        tprms=tprms_dict,
                    )
                    yield chunk_converted
        except AioRpcError:
            print(traceback.format_exc(), file=stderr)
            raise ValueError("Service error")

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

    def get_tmo_by_mo_id(self, mo_id: int) -> int:
        print("GRPC: get tmo by mo id")
        if mo_id <= 0:
            raise ValueError("Incorrect value of mo id.")
        try:
            query = InTmoByMoId(mo_id=mo_id)
            with self.lock:
                response: OutTmoId = self.stub.GetTmoByMoId(query)
            tmo_id = int(response.tmo_id)
            return tmo_id
        except grpc.RpcError as e:
            if e.code() == grpc.StatusCode.NOT_FOUND:
                raise ValueError(e.details())
            else:
                raise e

    def get_mos_by_mo_ids(self, mo_ids: list[int]) -> list[dict]:
        print("GRPC: get mos by mo ids")
        try:
            msg = InMOsByMoIds(mo_ids=mo_ids)
            with self.lock:
                response: OutMOsByMoIds = self.stub.GetMOsByMoIds(msg)
            mos = MessageToDict(
                response,
                always_print_fields_with_no_presence=True,
                preserving_proto_field_name=True,
            )["mos"]
            for mo in mos:
                mo["tmo_id"] = int(mo["tmo_id"])
                if "p_id" in mo:
                    mo["p_id"] = int(mo["p_id"])
                mo["id"] = int(mo["id"])
                mo["point_a_id"] = int(mo["point_a_id"])
                mo["point_b_id"] = int(mo["point_b_id"])
                mo["version"] = int(mo["version"])
            return mos

        except grpc.RpcError as e:
            if e.code() == grpc.StatusCode.NOT_FOUND:
                raise ValueError(e.details())
            else:
                raise e

    def get_prms_by_prm_ids(self, prm_ids: list[int]) -> list[dict]:
        print("GRPC: get prms by prm ids")
        try:
            msg = InPRMsByPRMIds(prm_ids=prm_ids)
            with self.lock:
                response: OutPRMsByPRMIds = self.stub.GetPRMsByPRMIds(msg)
            response: list[dict] = MessageToDict(
                response,
                always_print_fields_with_no_presence=True,
                preserving_proto_field_name=True,
            )
            tprms = self.get_tprms_by_tmo_id(
                tmo_ids=[int(i["tmo_id"]) for i in response]
            )
            tprms_dict = {i["id"]: i for i in tprms}
            results = self._convert_prm_val_type(
                prms=response, tprms=tprms_dict
            )
            return results
        except grpc.RpcError as e:
            if e.code() == grpc.StatusCode.NOT_FOUND:
                raise ValueError(e.details())
            else:
                raise e

    def get_point_tmo_const(self, tmo_id: int) -> list[int]:
        print("GRPC: get point tmo const")
        try:
            msg = InTmoId(tmo_id=tmo_id)
            try:
                with self.lock:
                    response: OutTmoIds = self.stub.GetPointTmoConst(msg)
            except BaseException as e:
                print(traceback.format_exc())
                raise e
            response: dict = MessageToDict(
                response,
                always_print_fields_with_no_presence=True,
                preserving_proto_field_name=True,
            )
            return response["tmo_ids"]
        except grpc.RpcError as e:
            if e.code() == grpc.StatusCode.NOT_FOUND:
                raise ValueError(e.details())
            else:
                raise e

    def get_tprm_const(self, tprm_id: int) -> list[int]:
        print("GRPC: get tprm const")
        try:
            msg = InTprmId(tprm_id=tprm_id)
            with self.lock:
                response: OutTmoIds = self.stub.GetPointTmoConst(msg)
            response: dict = MessageToDict(
                response,
                always_print_fields_with_no_presence=True,
                preserving_proto_field_name=True,
            )
            return response["tmo_ids"]
        except grpc.RpcError as e:
            if e.code() == grpc.StatusCode.INVALID_ARGUMENT:
                raise ValueError(e.details())
            elif e.code() == grpc.StatusCode.NOT_FOUND:
                raise ValueError(e.details())
            else:
                raise e

    def get_tprms_by_tprm_id(self, tprm_ids: list[int]) -> list[dict]:
        print("GRPC: get tprms by tprm id")
        msg = InTprmIds(tprm_ids=tprm_ids)
        with self.lock:
            response: OutTprms = self.stub.GetTprmByTprmIds(msg)
        response: list[dict] = MessageToDict(
            response,
            always_print_fields_with_no_presence=True,
            preserving_proto_field_name=True,
        )["tprms"]
        for tprm in response:
            tprm["id"] = int(tprm["id"])
            tprm["tmo_id"] = int(tprm["tmo_id"])
        return response
