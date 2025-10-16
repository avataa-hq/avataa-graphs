from typing import Literal

from pydantic import Field, computed_field, field_validator, model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict
from typing_extensions import Self


class InventoryGRPCConfig(BaseSettings):
    host: str = Field("inventory", min_length=1)
    port: int | None = Field(50051, ge=0)

    @computed_field
    @property
    def url(self) -> str:
        _url = self.host
        if self.port is not None:
            _url = f"{_url}:{self.port}"
        return _url

    model_config = SettingsConfigDict(env_prefix="inventory_grpc_")


class ArangoConfig(BaseSettings):
    protocol: Literal["http", "https"] = "http"
    host: str = Field("arangodb", min_length=1)
    port: int | None = Field(8529, ge=0)
    username: str = Field("graph", min_length=1)
    password: str = Field(..., min_length=1)

    @computed_field
    @property
    def url(self) -> str:
        _url = f"{self.protocol}://{self.host}"
        if self.port is not None:
            _url = f"{_url}:{self.port}"
        return _url

    model_config = SettingsConfigDict(env_prefix="arango_")


class GraphDBConfig(BaseSettings):
    sys_database_name: str = Field("_system")
    main_graph_collection_name: str = Field("main_graphs")

    db_name_prefix: str = Field("tmoId")
    collection_name_prefix: str = Field("tmoId")

    tmo_collection_name: str = Field("tmo")
    tmo_edge_name: str = Field("tmoEdge")
    tmo_graph_name: str = Field("tmoGraph")
    config_collection_name: str = Field("config")
    graph_data_collection_name: str = Field("main")
    graph_data_edge_name: str = Field("mainEdge")
    graph_data_graph_name: str = Field("mainGraph")
    graph_data_path_name: str = Field("pathEdge")
    graph_data_path_graph_name: str = Field("pathGraph")
    search_index_name: str = Field("inv-idx-name")
    search_index_indexed: str = Field("inv-idx-indexed")
    search_view: str = Field("search-view")

    def get_db_name(self, tmo_id: int) -> str:
        return f"{self.db_name_prefix}_{tmo_id}"

    def get_collection_name(self, tmo_id: int) -> str:
        return f"{self.collection_name_prefix}_{tmo_id}"

    def get_tmo_collection_key(self, tmo_id: int | str) -> str:
        return f"{self.tmo_collection_name}/{tmo_id}"

    def get_node_key(self, node_id: int | str) -> str:
        return f"{self.graph_data_collection_name}/{node_id}"

    model_config = SettingsConfigDict(env_prefix="graph_db_")


class CommonConfig(BaseSettings):
    """Consider data for common config in application."""

    DOCS_ENABLED: bool = Field(default=True)
    DOCS_CUSTOM_ENABLED: bool = Field(default=False)
    SWAGGER_JS_URL: str = Field(
        default="", validation_alias="DOCS_SWAGGER_JS_URL"
    )
    SWAGGER_CSS_URL: str = Field(
        default="", validation_alias="DOCS_SWAGGER_CSS_URL"
    )
    REDOC_JS_URL: str = Field(default="", validation_alias="DOCS_REDOC_JS_URL")


class AppConfig(BaseSettings):
    prefix: str = Field("/api/graph", pattern=r"^\/.+[^\/]$")

    model_config = SettingsConfigDict(env_prefix="app_")


class PathFinderConfig(BaseSettings):
    response_limit: int = Field(5, gt=0, le=10)
    search_limit: int = Field(20, gt=0, le=50)

    @model_validator(mode="after")
    def check(self) -> Self:
        if self.search_limit < self.response_limit:
            raise ValueError("Search limit must be more than response limit")
        return self


class SecurityConfig(BaseSettings):
    admin_role: str = Field(default="__admin")
    security_type: str = Field(default="KEYCLOAK-INFO")

    @field_validator("security_type", mode="before")
    @classmethod
    def normalize_security_type(cls, value: str) -> str:
        if isinstance(value, str):
            return value.upper()
        else:
            return value

    keycloak_protocol: Literal["http", "https"] = Field(default="http")
    keycloak_host: str = Field(
        default="keycloak", min_length=1, validation_alias="keycloak_host"
    )
    keycloak_port: int | None = Field(
        default=8080, gt=0, lt=65_536, validation_alias="keycloak_port"
    )
    keycloak_redirect_protocol_raw: Literal["http", "https", None] = Field(
        default=None, validation_alias="keycloak_redirect_protocol"
    )
    keycloak_redirect_host_raw: str | None = Field(
        default=None, min_length=1, validation_alias="keycloak_redirect_host"
    )
    keycloak_redirect_port_raw: int | None = Field(
        default=None, gt=0, validation_alias="keycloak_redirect_port"
    )
    realm: str = Field(
        default="avataa", min_length=1, validation_alias="keycloak_realm"
    )

    @computed_field  # type: ignore
    @property
    def keycloak_redirect_protocol(self) -> str:
        if self.keycloak_redirect_protocol_raw is None:
            return self.keycloak_protocol
        return self.keycloak_redirect_protocol_raw

    @computed_field  # type: ignore
    @property
    def keycloak_redirect_host(self) -> str:
        if self.keycloak_redirect_host_raw is None:
            return self.keycloak_host
        return self.keycloak_redirect_host_raw

    @computed_field  # type: ignore
    @property
    def keycloak_redirect_port(self) -> int:
        if self.keycloak_redirect_port_raw is None:
            return self.keycloak_port
        return self.keycloak_redirect_port_raw

    @computed_field  # type: ignore
    @property
    def keycloak_url(self) -> str:
        url = f"{self.keycloak_protocol}://{self.keycloak_host}"
        if self.keycloak_port:
            url = f"{url}:{self.keycloak_port}"
        return url

    @computed_field  # type: ignore
    @property
    def keycloak_public_key_url(self) -> str:
        return f"{self.keycloak_url}/realms/{self.realm}"

    @computed_field  # type: ignore
    @property
    def keycloak_redirect_url(self) -> str:
        url = (
            f"{self.keycloak_redirect_protocol}://{self.keycloak_redirect_host}"
        )
        if self.keycloak_redirect_port:
            url = f"{url}:{self.keycloak_redirect_port}"
        return url

    @computed_field  # type: ignore
    @property
    def keycloak_token_url(self) -> str:
        return f"{self.keycloak_redirect_url}/realms/{self.realm}/protocol/openid-connect/token"

    @computed_field  # type: ignore
    @property
    def keycloak_authorization_url(self) -> str:
        return f"{self.keycloak_redirect_url}/realms/{self.realm}/protocol/openid-connect/auth"

    opa_protocol: Literal["http", "https"] = Field(default="http")
    opa_host: str = Field(default="opa", min_length=1)
    opa_port: int = Field(default=8181, gt=0)
    opa_policy: str = Field(default="main")

    @computed_field  # type: ignore
    @property
    def opa_url(self) -> str:
        return f"{self.opa_protocol}://{self.opa_host}:{self.opa_port}"

    @computed_field  # type: ignore
    @property
    def opa_policy_path(self) -> str:
        return f"/v1/data/{self.opa_policy}"

    security_middleware_protocol: Literal["http", "https"] | None = Field(
        default="http", validation_alias="security_middleware_protocol"
    )
    security_middleware_host: str | None = Field(
        default="security-middleware", min_length=1
    )
    security_middleware_port: int | None = Field(default=8000, gt=0)

    @model_validator(mode="after")
    def set_defaults(self) -> "SecurityConfig":
        if self.security_middleware_protocol is None:
            self.security_middleware_protocol = self.keycloak_protocol
        if self.security_middleware_host is None:
            self.security_middleware_host = self.keycloak_host
        if self.security_middleware_port is None:
            self.security_middleware_port = self.keycloak_port
        return self

    @computed_field  # type: ignore
    @property
    def security_postfix(self) -> str:
        if (
            self.security_middleware_host == self.keycloak_host
            and self.security_middleware_port == self.keycloak_port
        ):
            url = f"/realms/{self.realm}/protocol/openid-connect/userinfo"
        else:
            url = f"/api/security_middleware/v1/cached/realms/{self.realm}/protocol/openid-connect/userinfo"
        return url

    @computed_field  # type: ignore
    @property
    def security_middleware_url(self) -> str:
        return (
            f"{self.security_middleware_protocol}://{self.security_middleware_host}:{self.security_middleware_port}"
            f"{self.security_postfix}"
        )
