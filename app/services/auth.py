import datetime
from datetime import timedelta

from keycloak import KeycloakOpenID


class KeycloakAuth:
    EXPIRE_LIMIT = 0.9

    def __init__(
        self,
        keycloak_open_id: KeycloakOpenID,
        username="",
        password="",
        grant_type: str | None = "password",
        scope="openid",
    ):
        self.keycloak_open_id = keycloak_open_id
        self.__username = username
        self.__password = password
        self.__grant_type = grant_type
        self.__scope = scope

        self.update_token_after: datetime.datetime = (
            datetime.datetime.now() - timedelta(days=1)
        )
        self.update_refresh_token_after: datetime.datetime = (
            datetime.datetime.now() - timedelta(days=1)
        )
        self.token: str = ""
        self.refresh_token: str = ""

    def _get_update_datetime(
        self, expire_in: int, now: datetime.datetime | None = None
    ) -> datetime.datetime:
        if expire_in < 0:
            raise ValueError(f"Incorrect value of {expire_in=}")
        if 1 < self.EXPIRE_LIMIT < 0:
            raise ValueError(f"Incorrect value of {self.EXPIRE_LIMIT=}")
        if not now:
            now = datetime.datetime.now()
        expire_in_limit = expire_in * self.EXPIRE_LIMIT
        expire_datetime = now + timedelta(seconds=expire_in_limit)
        return expire_datetime

    def get_token(self):
        now = datetime.datetime.now()
        if now <= self.update_token_after:
            return self.token
        if now > self.update_refresh_token_after:
            token_response = self.keycloak_open_id.token(
                username=self.__username,
                password=self.__password,
                grant_type=self.__grant_type,
                scope=self.__scope,
            )
        else:
            token_response = self.keycloak_open_id.refresh_token(
                refresh_token=self.refresh_token
            )
        self.token = token_response["access_token"]
        self.refresh_token = token_response["refresh_token"]
        self.update_token_after = self._get_update_datetime(
            expire_in=token_response["expires_in"]
        )
        self.update_refresh_token_after = self._get_update_datetime(
            expire_in=token_response["refresh_expires_in"]
        )

        return self.token
