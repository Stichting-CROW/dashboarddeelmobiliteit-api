import json
from bson import json_util
import psycopg2.extras

class Trips():
    def __init__(self, conn):
        self.conn = conn

    def get_trips(self, d_filter):
        cur = self.conn.cursor()
        stmt = """ 
        WITH temp_a (filter_area) AS
            (
            SELECT st_union(area) 
	            FROM zones WHERE zone_id IN %s
        )

        SELECT system_id, bike_id, st_y(start_location), st_x(start_location), 
        st_y(end_location), st_x(end_location),  start_time, end_time, trip_id 
        FROM trips, temp_a
        WHERE 
        start_time >= %s
        AND end_time <= %s
        AND (false = %s or 
            (ST_Within(start_location, temp_a.filter_area) OR
             ST_Within(end_location, temp_a.filter_area) )) 
        """
        cur.execute(stmt, (d_filter.get_zones(), d_filter.get_start_time(), 
            d_filter.get_end_time(), d_filter.has_zone_filter()))
        return self.serialize_trips(cur.fetchall())

    def serialize_trips(self, trips):
        result = []
        for trip in trips:
            result.append(self.serialize_trip(trip))

        return result
        
    def serialize_trip(self, trip):
        data = {}
        data["system_id"] = trip[0]
        data["bike_id"] = trip[1]
        data["start_location"] = {}
        data["start_location"]["latitude"] = trip[2] 
        data["start_location"]["longitude"] = trip[3]
        data["end_location"] = {}
        data["end_location"]["latitude"] = trip[4] 
        data["end_location"]["longitude"] = trip[5]
        data["start_time"] = trip[6]
        data["end_time"] = trip[7]
        data["trip_id"] = trip[8]
        return data
       


