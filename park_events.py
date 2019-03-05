import json
from bson import json_util
import psycopg2.extras

class ParkEvents():
    def __init__(self, conn):
        self.conn = conn

    def get_park_events(self, d_filter):
        cur = self.conn.cursor()
        stmt = """ 
        SELECT system_id, bike_id, 
	        ST_Y(location), ST_X(location), 
	        start_time, end_time
        FROM park_events
        WHERE start_time < %s 
            AND (end_time > %s OR end_time is null)
        AND (false = %s or ST_WITHIN(location, 
	    (SELECT st_union(area) 
	    FROM zones WHERE zone_id IN %s)));
        """
        cur.execute(stmt, (d_filter.get_timestamp(), d_filter.get_timestamp(),
            d_filter.has_zone_filter(), d_filter.get_zones() ))
        return self.serialize_park_events(cur.fetchall())

    def serialize_park_events(self, park_events):
        result = []
        for park_event in park_events:
            result.append(self.serialize_park_event(park_event))

        return result
        
    def serialize_park_event(self, park_event):
        data = {}
        data["system_id"] = park_event[0]
        data["bike_id"] = park_event[1]
        data["location"] = {}
        data["location"]["latitude"] = park_event[2] 
        data["location"]["longitude"] = park_event[3]
        data["start_time"] = park_event[4]
        data["end_time"] = park_event[5]
        return data
       


