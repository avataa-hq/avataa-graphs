import functools
import time
from typing import Callable, Literal

from pydantic import Field, SecretStr, computed_field
from pydantic_settings import BaseSettings, SettingsConfigDict
import requests


class KafkaTopicsConfig(BaseSettings):
    inventory: str = Field(
        "inventory.changes", validation_alias="kafka_inventory_changes_topic"
    )


class KeycloakConnectionConfig(BaseSettings):
    scopes: str = Field("profile", validation_alias="kafka_keycloak_scopes")
    client_id: str = Field(
        "kafka", min_length=1, validation_alias="kafka_keycloak_client_id"
    )
    client_secret: SecretStr | None = Field(
        None, validation_alias="kafka_keycloak_client_secret"
    )
    protocol: Literal["http", "https"] = Field(
        "http", validation_alias="keycloak_protocol"
    )
    host: str = Field(
        "localhost", min_length=1, validation_alias="keycloak_host"
    )
    port: int | None = Field(None, gt=0, validation_alias="keycloak_port")
    realm: str = Field(
        "master", min_length=1, validation_alias="keycloak_realm"
    )

    @computed_field
    @property
    def url(self) -> str:
        url = f"{self.protocol}://{self.host}"
        if self.port:
            url = f"{url}:{self.port}"
        return url

    @computed_field
    @property
    def token_url(self) -> str:
        return f"{self.url}/realms/{self.realm}/protocol/openid-connect/token"


class KafkaConnectionConfig(BaseSettings):
    bootstrap_servers: str = Field(
        "kafka:9092",
        serialization_alias="bootstrap.servers",
        validation_alias="kafka_url",
        min_length=1,
    )
    group_id: str = Field("Graph", serialization_alias="group.id", min_length=1)
    auto_offset_reset: Literal["earliest", "latest", "none"] = Field(
        "latest",
        serialization_alias="auto.offset.reset",
        validation_alias="kafka_consumer_offset",
    )
    enable_auto_commit: bool = Field(
        False, serialization_alias="enable.auto.commit"
    )
    security_protocol: Literal["sasl_plaintext", None] = Field(
        None, serialization_alias="security.protocol"
    )
    sasl_mechanism: Literal["OAUTHBEARER", None] = Field(
        None, serialization_alias="sasl.mechanisms"
    )

    @computed_field
    @property
    def oauth_cb(self) -> None | Callable:
        if not self.sasl_mechanism:
            return None
        keycloak_config = KeycloakConnectionConfig()
        return functools.partial(
            self._get_token_for_kafka_producer,
            keycloak_config=keycloak_config,
            group=self.group_id,
        )

    @staticmethod
    def _get_token_for_kafka_producer(
        conf,
        keycloak_config: KeycloakConnectionConfig,
        group: str,
    ) -> tuple[str, float]:
        """Get token from Keycloak for MS Inventory kafka producer and returns it with expires time"""
        payload = {
            "grant_type": "client_credentials",
            "scope": keycloak_config.scopes,
        }
        attempt = 5
        while attempt > 0:
            try:
                resp = requests.post(
                    keycloak_config.token_url,
                    auth=(
                        keycloak_config.client_id,
                        keycloak_config.client_secret.get_secret_value()
                        if keycloak_config.client_secret
                        else None,
                    ),
                    data=payload,
                    timeout=5,
                )
            except ConnectionError:
                time.sleep(1)
                attempt -= 1
            else:
                if resp.status_code == 200:
                    break
                else:
                    time.sleep(1)
                    attempt -= 1
                    continue
        else:
            raise PermissionError("Token verification service unavailable")

        token = resp.json()
        expires_in = float(token["expires_in"]) * 0.9
        return token["access_token"], time.time() + expires_in

    model_config = SettingsConfigDict(env_prefix="kafka_")
