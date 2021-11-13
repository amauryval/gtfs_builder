import psycopg2
import psycopg2.extras

import itertools


def sql_query_to_list(query):
    return [
        {
            column: getattr(row, column)
            for column in row._fields
        }
        for row in query.all()
    ]

def querying(connection, query):
    cursor = connection.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
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
        nodes_query = f"""
        SELECT 
            ST_X(geometry) as x,
            ST_Y(geometry) as y, 
            stop_name,
            route_type        
        FROM 
            gtfs_data.stops_times_{self._area}
        WHERE
            start_date <= '{current_date}' AND end_date >= '{current_date}'
        """
        # print(nodes_query)
        nodes_res = querying(self._connection, nodes_query)

        return {
            "data_geojson": nodes_res
        }


    def context_data_from_db(self):

        bounds_query = f"""
        SELECT 
            ST_EXTENT(geometry) as extent
        FROM 
            gtfs_data.stops_times_{self._area}
        WHERE
            pos = 0
        """
        bounds_res = querying(self._connection, bounds_query)

        date_bounds_query = f"""
        SELECT 
            min(start_date) as start_date,
            max(end_date) as end_date
        FROM 
            gtfs_data.stops_times_{self._area}
        """
        date_bounds_res = querying(self._connection, date_bounds_query)

        return {
            "data_bounds": list(map(lambda x: float(x), list(itertools.chain(*list(map(lambda x: x.split(" "), bounds_res[0]["extent"].replace("BOX(", "").replace(")", "").split(','))))))),
            "start_date": date_bounds_res[0]["start_date"].strftime(self.__DATE_FORMAT),
            "end_date": date_bounds_res[0]["end_date"].strftime(self.__DATE_FORMAT),
        }