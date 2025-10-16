from task.models.incoming_data import MO, PRM, TMO, TPRM
from updater.converters.inventory.message_converters.message_converter_abstract import (
    MessageConverterAbstract,
)
from updater.converters.inventory.proto.inventory_instances_pb2 import (
    ListMO,
    ListPRM,
    ListTMO,
    ListTPRM,
)  # noqa


class MOConverter(MessageConverterAbstract):
    PREFIX = "MO"
    GRPC_CLASS = ListMO
    DTO_CLASS = MO


class TMOConverter(MessageConverterAbstract):
    PREFIX = "TMO"
    GRPC_CLASS = ListTMO
    DTO_CLASS = TMO


class TPRMConverter(MessageConverterAbstract):
    PREFIX = "TPRM"
    GRPC_CLASS = ListTPRM
    DTO_CLASS = TPRM


class PRMConverter(MessageConverterAbstract):
    PREFIX = "PRM"
    GRPC_CLASS = ListPRM
    DTO_CLASS = PRM


class DefaultConverter(MessageConverterAbstract):
    PREFIX = ""
