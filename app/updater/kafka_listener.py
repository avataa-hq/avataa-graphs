"""
Класс слушателя кафки
В идеале на события из кафки другие сервисы могут подписываться и получать свои данные
Например, подписка на ТМО, либо на все сразу
"""

from abc import ABC, abstractmethod
from collections import defaultdict
from functools import partial
import sys

from confluent_kafka import Consumer, cimpl

from updater.converters.inventory.inventory_changes_topic import (
    ParsedMessage,
    TopicConverter,
)
from updater.updater_config import KafkaConnectionConfig


class TopicSubscriber(ABC):
    @abstractmethod
    def send_message(self, message: ParsedMessage):
        raise NotImplementedError()


class KafkaListener:
    def __init__(self, group_postfix: str = ""):
        self.group_postfix = group_postfix
        self._is_started: bool = False
        self._consumer: Consumer | None = None
        self._topic_converters: dict[str, TopicConverter] = {}
        self._subscribers: dict[str, list[TopicSubscriber]] = defaultdict(list)
        self._default_topic_converter: TopicConverter = TopicConverter(
            "default"
        )

    @property
    def consumer(self):
        if not self._consumer:
            config = KafkaConnectionConfig()
            consumer_config = config.model_dump(
                by_alias=True,
                exclude_none=True,
                include={
                    "auto_offset_reset",
                    "bootstrap_servers",
                    "enable_auto_commit",
                    "group_id",
                },
            )
            if config.sasl_mechanism:
                consumer_config.update(
                    {
                        "oauth_cb": config.oauth_cb,
                        "error_cb": partial(self._error_cb),
                        "sasl.mechanisms": config.sasl_mechanism,
                        "security.protocol": config.security_protocol,
                    }
                )
            if self.group_postfix:
                consumer_config["group.id"] = (
                    f"""{consumer_config["group.id"]}_{self.group_postfix}"""
                )
            self._consumer = Consumer(consumer_config)
        return self._consumer

    @staticmethod
    def _error_cb(ex: Exception) -> None:
        print(ex)

    def add_topic_converter(self, converter: TopicConverter):
        self._topic_converters[converter.topic] = converter

    def remove_topic_converter(self, topic: str):
        if topic in self._topic_converters:
            del self._topic_converters[topic]

    def get_topic_converters_name(self) -> list[str]:
        return list(self._topic_converters.keys())

    def get_topic_converter(self, topic: str) -> TopicConverter:
        return self._topic_converters.get(topic, self._default_topic_converter)

    def get_topics_from_subscribers(self) -> list[str]:
        return list(self._subscribers.keys())

    def subscribe(self, topic: str, subscriber: TopicSubscriber):
        if not topic or not isinstance(topic, str):
            return
        if not subscriber or not isinstance(subscriber, TopicSubscriber):
            return
        subscribers = self._subscribers[topic]
        if subscriber not in subscribers:
            subscribers.append(subscriber)

    def _send_message(self, topic: str, message: ParsedMessage):
        for subscriber in self._subscribers[topic]:
            subscriber.send_message(message=message)

    def stop(self):
        self._is_started = False

    def convert_message(self, message: cimpl.Message) -> ParsedMessage | None:
        if message is None:
            return
        if message.error():
            print(str(message.error()), file=sys.stderr)
            return
        topic = message.topic()
        converter = self.get_topic_converter(topic)
        converted_msg: ParsedMessage = converter.convert_message(
            message=message
        )
        if converted_msg:
            return converted_msg

    def start(self):
        if self._is_started:
            return
        self._is_started = True
        topics = self.get_topics_from_subscribers()
        if not topics:
            return
        self.consumer.subscribe(topics=topics)
        while self._is_started:
            msg = self.consumer.poll(timeout=60)
            if not msg:
                continue
            converted_msg: ParsedMessage | None = self.convert_message(
                message=msg
            )
            if converted_msg is None:
                continue
            self._send_message(msg.topic(), converted_msg)
            self.consumer.commit(msg)

        self.consumer.unsubscribe()
