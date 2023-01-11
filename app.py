import uvicorn
from starlite import Starlite, CORSConfig, OpenAPIConfig, Router

from gtfs_builder.app.config import settings
from gtfs_builder.app.helpers import input_data
from gtfs_builder.app.routes_from_db import db_routes
from gtfs_builder.app.routes_from_files import file_routes


def set_app() -> Starlite:
    open_api_enabled = False

    if settings.MODE == "file":
        base_route = Router(path=settings.API_PREFIX, route_handlers=[file_routes])
        initial_state = {"geodata": input_data()}
    else:
        base_route = Router(path=settings.API_PREFIX, route_handlers=[db_routes])
        initial_state = None

    application = Starlite(
        openapi_config=OpenAPIConfig(
            title=settings.PROJECT_NAME, version=settings.PROJECT_NAME
        ) if open_api_enabled else None,
        route_handlers=[base_route],
        cors_config=CORSConfig(allow_origins=settings.ORIGINS),
        initial_state=initial_state
    )

    return application


app = set_app()

if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=7000)
