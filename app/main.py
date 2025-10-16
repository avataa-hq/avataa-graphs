from starlette.middleware.cors import CORSMiddleware

from config import AppConfig
from init_app import create_app
from v1 import app_v1

app = create_app(root_path=AppConfig().prefix)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
    expose_headers=["*"],
)

app.mount("/v1", app_v1)
