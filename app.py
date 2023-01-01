import uvicorn
from starlite import Starlite, CORSConfig, OpenAPIConfig, Router, Provide

from gtfs_builder.app.config import settings
from gtfs_builder.app.helpers import input_data
from gtfs_builder.app.routes_from_files import file_routes, existing_study_areas


def set_app() -> Starlite:
    open_api_enabled = False

    # file_db_route = Router(path=settings.API_PREFIX, route_handlers=portfolio_routes)
    file_base_route = Router(path=settings.API_PREFIX, route_handlers=[file_routes, existing_study_areas])

    application = Starlite(
        openapi_config=OpenAPIConfig(title=settings.PROJECT_NAME, version=settings.PROJECT_NAME) if open_api_enabled else None,
        route_handlers=[file_base_route],
        cors_config=CORSConfig(allow_origins=settings.ORIGINS),
        dependencies={"input_data": Provide(input_data, use_cache=True)}
    )

    return application


app = set_app()

if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=7000)

