from spatialpandas import GeoDataFrame

from gtfs_builder.app.config import settings

from gtfs_builder.app.core import GtfsMain

from gtfs_builder.app.helpers import input_data

from typing import List, Dict

from starlite import Router, Provide, Controller, get


class FromFileController(Controller):

    @get("/{area:str}/moving_nodes_by_date")
    async def moving_nodes_by_date(self,
                                   area: str,
                                   input_data: dict,
                                   bounds: str,
                                   current_date: str,
                                   route_type: str | None = None) -> list[dict]:

        return GtfsMain(input_data[area]).nodes_by_date_from_parquet(
            current_date,
            list(map(lambda x: float(x), bounds.split(","))),
            route_type
        )

    @get("/{area:str}/range_dates")
    async def range_dates(self, area: str, input_data: dict) -> dict:
        return GtfsMain(input_data[area]).context_data_from_parquet()

    @get("/{area:str}/route_types")
    async def transport_types(self, area: str, input_data: dict) -> list[str]:
        return GtfsMain(input_data[area]).transport_types_from_parquet()

    @get("/existing_study_areas")
    async def existing_study_areas(self) -> list[str]:
        return settings.AREAS

file_routes = Router(
    path="/",
    route_handlers=[FromFileController],
    # dependencies={"input_data": Provide(input_data, use_cache=True)}
)


