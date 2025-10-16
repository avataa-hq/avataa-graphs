from collections import namedtuple

from confluent_kafka import cimpl

from updater.converters.inventory.message_converters.inventory_converter import (
    DefaultConverter,
    MOConverter,
    PRMConverter,
    TMOConverter,
    TPRMConverter,
)
from updater.converters.inventory.message_converters.message_converter_abstract import (
    MessageConverterAbstract,
)

ParsedMessage = namedtuple("ParsedMessage", ["key", "value"])


class TopicConverter:
    def __init__(self, topic: str):
        self.topic = topic
        self.message_converters: dict[str, MessageConverterAbstract] = {
            MOConverter.PREFIX: MOConverter(),
            TMOConverter.PREFIX: TMOConverter(),
            TPRMConverter.PREFIX: TPRMConverter(),
            PRMConverter.PREFIX: PRMConverter(),
        }
        self.default_converter = DefaultConverter()
        self.sep = ":"

    def find_appropriate_converter(self, key: str) -> MessageConverterAbstract:
        prefix = key.split(sep=self.sep, maxsplit=1)[0]
        return self.message_converters.get(prefix, self.default_converter)

    def convert_message(self, message: cimpl.Message) -> ParsedMessage | None:
        key = message.key().decode("utf-8")
        converter = self.find_appropriate_converter(key=key)
        value = converter.parse_message(message=message.value())  # noqa
        if value:
            return ParsedMessage(key=key, value=value)
