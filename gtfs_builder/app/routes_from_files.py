from spatialpandas import GeoDataFrame

from gtfs_builder.app.config import settings

from gtfs_builder.app.core import GtfsMain

from gtfs_builder.app.helpers import get_data

from typing import List

from starlite import Router, Provide, Controller, get


class FromFileController(Controller):
    @get("/existing_study_areas")
    def existing_study_areas(self) -> list[str]:
        return settings.AREAS

    @get("/{area:str}/moving_nodes_by_date", dependencies={"input_data": Provide(get_data)})
    def moving_nodes_by_date(self,
                             input_data: GeoDataFrame,
                             bounds: str,
                             current_date: str,
                             route_type: str | None = None) -> list[dict]:

        return GtfsMain(input_data).nodes_by_date_from_parquet(
            current_date,
            list(map(lambda x: float(x), bounds.split(","))),
            route_type
        )

    @get("/{area:str}/range_dates", dependencies={"input_data": Provide(get_data)})
    def range_dates(self, input_data: GeoDataFrame) -> dict:
        return GtfsMain(input_data).context_data_from_parquet()

    @get("/{area:str}/route_types", dependencies={"input_data": Provide(get_data)})
    def transport_types(self, input_data: GeoDataFrame) -> list[str]:
        return GtfsMain(input_data).transport_types_from_parquet()


file_routes = Router(
    path="/",
    route_handlers=[FromFileController],
)

