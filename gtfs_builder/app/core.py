import psycopg2
import psycopg2.extras

import itertools

from gtfs_builder.db.stops_times_ter import StopsTimesTer
from geoalchemy2 import func
from sqlalchemy import and_


def sql_query_to_list(query):
    return [
        {
            column: getattr(row, column)
            for column in row._fields
        }
        for row in query.all()
    ]

def querying(connection, query):
    cursor = connection.cursor()
    cursor.execute(query)
    result = [dict(row) for row in cursor.fetchall()]

    cursor.close()
    return result



class GtfsMain:
    __DATE_FORMAT = "%Y-%m-%d %H:%M:%S"

    def __init__(self, conn, area):

        self._area = area
        self._connection = conn

    def nodes_by_date_from_db(self, current_date, bounds):

        bounds = list(bounds)
        query = self._connection.query(
            func.ST_X(StopsTimesTer.geom).label("x"),
            func.ST_Y(StopsTimesTer.geom).label("y"),
            StopsTimesTer.stop_name.label("stop_name"),
            StopsTimesTer.route_type.label("route_type"),
        ).filter(
            and_(
                StopsTimesTer.start_date <= current_date,
                StopsTimesTer.end_date >= current_date,
            )
        )
        assert True

        return {
            "data_geojson": sql_query_to_list(query)
        }


        # nodes_query = f"""
        # SELECT
        #     X(geom) as x,
        #     Y(geom) as y,
        #     stop_name,
        #     route_type
        # FROM
        #     stops_times_{self._area}
        # WHERE
        #     start_date <= '{current_date}' AND end_date >= '{current_date}'
        # """
        # # print(nodes_query)
        # nodes_res = querying(self._connection, nodes_query)
        #
        # return {
        #     "data_geojson": nodes_res
        # }


    def context_data_from_db(self):


        # query_range_date = self._connection.query(
        #     min(StopsTimesTer.start_date).label("start_date"),
        #     max(StopsTimesTer.end_date).label("end_date"),
        # )
        # assert True
        # date_range = sql_query_to_list(query_range_date)
        assert True
        return {
            "data_bounds": [-14.765625, 39.520992, 21.774902, 53.173119],
            "start_date": "2021-11-25 00:03:00",
            "end_date": "2021-11-26 01:00:00",
        }