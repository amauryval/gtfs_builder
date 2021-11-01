

from geolib import GeoLib

from sqlalchemy import and_

from sqlalchemy.sql.expression import literal
from sqlalchemy.sql.expression import literal_column
from sqlalchemy import func

import datetime

def sql_query_to_list(query):
    return [
        {
            column: getattr(row, column)
            for column in row._fields
        }
        for row in query.all()
    ]


class GtfsMain(GeoLib):
    __DATE_FORMAT = "%Y-%m-%d %H:%M:%S"

    def __init__(self, session, engine=None, stops_geom_table=None, stops_times_values_table=None):
        super().__init__(logger_name=None)

        self._session = session
        self._engine = engine

        self.stops_geom_table = stops_geom_table
        self.stops_times_values_table = stops_times_values_table

    def context_data_from_parquet(self):
        return {
            "data_bounds": self._session.geometry.total_bounds,
            "start_date": datetime.datetime.fromtimestamp(min(self._session["start_date"])).strftime(self.__DATE_FORMAT),
            "end_date": datetime.datetime.fromtimestamp(max(self._session["end_date"])).strftime(self.__DATE_FORMAT),
        }

    def context_data_from_db(self):

        start_date = self.stops_times_values_table.query(
            func.to_char(func.min(func.lower(self.stops_times_values_table.validity_range)), "YYYY-MM-DD HH12:MI:SS").label("start_date")
        )

        end_date = self.stops_times_values_table.query(
            func.to_char(func.max(func.upper(self.stops_times_values_table.validity_range)), "YYYY-MM-DD HH12:MI:SS").label("end_date")
        )

        return {
            "start_date": sql_query_to_list(start_date)[0]["start_date"],
            "end_date": sql_query_to_list(end_date)[0]["end_date"],
        }

    def nodes_by_date_from_db(self, current_date):
        current_date = datetime.datetime.fromisoformat(current_date)

        data = self.stops_geom_table.query(
            self.stops_times_values_table.stop_code.label("stop_code"),
            # StopsGeom.stop_name.label("stop_name"),
            # StopsGeom.stop_type.label("stop_type"),
            # StopsGeom.line_name.label("line_name"),
            # StopsTimesValues.direction_id.label("direction_id"),
            self.stops_geom_table.line_name_short.label("line_name_short"),
            # StopsTimesValues.validity_range.label("validity_range"),
            func.ST_X(func.ST_ReducePrecision(self.stops_geom_table.geometry, 0.001)).label("x"),
            func.ST_Y(func.ST_ReducePrecision(self.stops_geom_table.geometry, 0.001)).label("y")
        ).filter(
            and_(
                self.stops_geom_table.stop_code == self.stops_times_values_table.stop_code,
                self.stops_times_values_table.validity_range.op('@>')(current_date)
            )
        )


        return {"data_geojson": sql_query_to_list(data)}

    def _create_geojson(self, properties_query, geometry_query, join_field):
        properties_subquery = properties_query.subquery('features')
        geometry_subquery = geometry_query.subquery('geometry')

        features = self._session.query(
            literal('Feature').label('type'),
            func.row_to_json(literal_column('features')).label('properties'),
            geometry_subquery.c.geometry.label('geometry')
        ).filter(
            getattr(geometry_subquery.c, join_field) == getattr(properties_subquery.c, join_field)
        ).subquery('features')

        geojson_features = self._session.query(
            func.jsonb_agg(literal_column('features'))
        ).select_from(
            features
        )
        return geojson_features

    def nodes_by_date_from_parquet(self, current_date):
        current_date = datetime.datetime.fromisoformat(current_date).timestamp()
        filtered_data = self._session.loc[(self._session["start_date"] <= current_date) & (self._session["end_date"] >= current_date)]
        filtered_data = filtered_data[["stop_code", "x", "y", "line_name_short"]]

        return {
            "data_geojson": filtered_data.to_dict("records")
        }
