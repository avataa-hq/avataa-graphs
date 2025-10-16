from config import AppConfig
from init_app import create_app
from routers import analysis, building, initialisation, search, tmo, tmp, trace

app_v1 = create_app(
    root_path=f"{AppConfig().prefix}/v1", title="Graph", version="1"
)

app_v1.include_router(initialisation.router)
app_v1.include_router(tmo.router)
app_v1.include_router(building.router)
app_v1.include_router(analysis.router)
app_v1.include_router(trace.router)
app_v1.include_router(search.router)
app_v1.include_router(tmp.router)
