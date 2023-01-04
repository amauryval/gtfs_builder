from starlite import Router, Provide, Controller, get

from gtfs_builder.app.config import settings
from gtfs_builder.app.core import GtfsMain
from gtfs_builder.app.helpers import input_data


class FromFileController(Controller):

    @get("/{area:str}/moving_nodes_by_date")
    def moving_nodes_by_date(self,
                             area: str,
                             geodata: dict,
                             bounds: str,
                             current_date: int,
                             route_type: str | None = None) -> any:

        return GtfsMain(geodata[area]).nodes_by_date_from_parquet(
            current_date,
            list(map(lambda x: float(x), bounds.split(","))),
            route_type
        )

    @get("/{area:str}/range_dates")
    def range_dates(self, area: str, geodata: dict) -> dict:
        return GtfsMain(geodata[area]).context_data_from_parquet()

    @get("/{area:str}/route_types")
    def transport_types(self, area: str, geodata: dict) -> list[str]:
        return GtfsMain(geodata[area]).transport_types_from_parquet()

    @get("/existing_study_areas")
    def existing_study_areas(self) -> list[str]:
        return settings.AREAS


file_routes = Router(
    path="/",
    route_handlers=[FromFileController],
    dependencies={"geodata": Provide(input_data, use_cache=True)}
)
