from abc import ABC, abstractmethod
import json
from typing import Type

from google.protobuf.descriptor import Descriptor, FieldDescriptor
from google.protobuf.json_format import MessageToDict
from google.protobuf.message import DecodeError, Message
from pydantic import BaseModel


class MessageConverterAbstract(ABC):
    FIELD_DESCRIPTOR_PARSERS = {FieldDescriptor.TYPE_INT64: int}
    GRPC_CLASS: Type[Message] | None = None
    DTO_CLASS: BaseModel | None = None

    def __init__(self):
        self.field_parsers: dict = self.__get_parsers(msg_type=self.GRPC_CLASS)

    def __get_parsers(self, msg_type: Type[Message]) -> dict:
        field_parsers = {}
        if not msg_type:
            return field_parsers

        if isinstance(msg_type, Descriptor):
            fields = msg_type.fields
        else:
            fields = msg_type.DESCRIPTOR.fields

        for field in fields:
            if field.type in self.FIELD_DESCRIPTOR_PARSERS:
                parser = self.FIELD_DESCRIPTOR_PARSERS[field.type]
            elif (
                field.type == FieldDescriptor.TYPE_MESSAGE
                and field.message_type
                and not field.message_type.full_name.startswith(
                    "google.protobuf."
                )
            ):
                parser = self.__get_parsers(msg_type=field.message_type)
            else:
                continue
            field_parsers[field.name] = parser
        return field_parsers

    @property
    @abstractmethod
    def PREFIX(self) -> str:
        raise NotImplementedError()

    def check_prefix(self, key: str):
        return key.startswith(self.PREFIX)

    def _parse_other_values(self, data: dict, msg_level_parser: dict) -> dict:
        for key, parser in msg_level_parser.items():
            value = data[key]
            if not value:
                continue
            if isinstance(parser, dict):
                data[key] = (
                    [
                        self._parse_other_values(
                            data=i, msg_level_parser=parser
                        )
                        for i in value
                    ]
                    if isinstance(value, list)
                    else self._parse_other_values(
                        data=value, msg_level_parser=parser
                    )
                )
            else:
                data[key] = (
                    [parser(i) for i in value]
                    if isinstance(value, list)
                    else parser(value)
                )
        return data

    def convert_from_grpc_to_dict(
        self, message: Message | bytes
    ) -> dict | None:
        if not self.GRPC_CLASS:
            if isinstance(message, bytes):
                try:
                    return self.convert_from_str_to_dict(
                        message=message.decode("utf-8")
                    )
                except UnicodeDecodeError:
                    return None
            raise NotImplementedError("gRPC class not set")
        try:
            grpc_message = self.GRPC_CLASS()
            grpc_message.ParseFromString(message)
            grpc_parsed: dict = MessageToDict(
                message=grpc_message,
                always_print_fields_with_no_presence=True,
                preserving_proto_field_name=True,
            )
            grpc_parsed = self._parse_other_values(
                data=grpc_parsed, msg_level_parser=self.field_parsers
            )
        except DecodeError:
            raise NotImplementedError("gRPC class realisation changed")
        else:
            return grpc_parsed

    def convert_from_str_to_dict(self, message: str) -> dict:
        return json.loads(message)

    def convert_from_dict_to_dto(self, data: dict | list):
        if not self.DTO_CLASS:
            return data
        data = data.get("objects", data)
        if isinstance(data, list):
            return [self.DTO_CLASS.model_validate(i) for i in data]
        return self.DTO_CLASS.model_validate(data)

    def parse_message(self, message: Message | str | dict | bytes):
        if isinstance(message, Message | bytes):
            data: dict | list = self.convert_from_grpc_to_dict(message=message)
        elif isinstance(message, str):
            data: dict | list = self.convert_from_str_to_dict(message=message)
        elif message is None:
            return message
        else:
            data: dict | list = message
        return self.convert_from_dict_to_dto(data)
