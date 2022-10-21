import json
from bson import json_util
import psycopg2.extras
import zones

class Rentals():
    def __init__(self):
        self.zones = zones.Zones()

    def get_start_trips(self, conn, d_filter):
        cur = conn.cursor()
        stmt = """ 
        WITH temp_a (filter_area) AS
            (
            SELECT st_union(area) 
	            FROM zones WHERE zone_id IN %s
        )

        SELECT system_id, bike_id, st_y(location), st_x(location), start_time
        FROM park_events, temp_a
        WHERE 
        end_time >= %s
        AND end_time <= %s
        AND (false = %s or 
            (ST_Within(location, temp_a.filter_area)))
        AND (false = %s or system_id IN %s) 
        """
        cur.execute(stmt, (d_filter.get_zones(), d_filter.get_start_time(), 
            d_filter.get_end_time(), d_filter.has_zone_filter(),
            d_filter.has_operator_filter(), d_filter.get_operators()))
        return self.serialize_rentals(cur.fetchall(), False)

    def get_end_trips(self, conn, d_filter):
        cur = conn.cursor()
        stmt = """ 
        WITH temp_a (filter_area) AS
            (
            SELECT st_union(area) 
	            FROM zones WHERE zone_id IN %s
        )

        SELECT system_id, bike_id, st_y(location), st_x(location), end_time
        FROM park_events, temp_a
        WHERE 
        start_time >= %s
        AND start_time <= %s
        AND (false = %s or 
            (ST_Within(location, temp_a.filter_area)))
        AND (false = %s or system_id IN %s) 
        """
        cur.execute(stmt, (d_filter.get_zones(), d_filter.get_start_time(), 
            d_filter.get_end_time(), d_filter.has_zone_filter(),
            d_filter.has_operator_filter(), d_filter.get_operators()))
        return self.serialize_rentals(cur.fetchall(), True)

    def query_stats_start_trip(self, conn, zone_id, d_filter):
        cur = conn.cursor()
        stmt = """WITH temp_a (filter_area) AS
            (SELECT st_union(area) 
	            FROM zones WHERE zone_id = %s)

            SELECT SUM(CASE WHEN ST_Within(location, temp_a.filter_area) THEN 1 ELSE 0 END)
            FROM park_events, temp_a
            WHERE 
            end_time >= %s
            AND end_time <= %s
            AND (ST_Within(location, temp_a.filter_area))
            AND (false = %s or system_id IN %s) ;
        """
        cur.execute(stmt, (zone_id, 
            d_filter.get_start_time(), d_filter.get_end_time(),
            d_filter.has_operator_filter(), d_filter.get_operators()))

        result = cur.fetchone()
        if (result):
            return result[0]
        return 0

    def query_stats_end_trip(self, conn, zone_id, d_filter):
        cur = conn.cursor()
        stmt = """WITH temp_a (filter_area) AS
            (SELECT st_union(area) 
	            FROM zones WHERE zone_id = %s)

            SELECT SUM(CASE WHEN ST_Within(location, temp_a.filter_area) THEN 1 ELSE 0 END)
            FROM park_events, temp_a
            WHERE 
            start_time >= %s
            AND start_time <= %s
            AND (ST_Within(location, temp_a.filter_area))
            AND (false = %s or system_id IN %s) ;
        """
        cur.execute(stmt, (zone_id, 
            d_filter.get_start_time(), d_filter.get_end_time(),
            d_filter.has_operator_filter(), d_filter.get_operators()))

        result = cur.fetchone()
        if (result):
            return result[0]
        return 0

    def query_stats(self, conn, zone_id, d_filter):
        result = {}
        values = [self.query_stats_end_trip(conn, zone_id, d_filter), self.query_stats_start_trip(conn, zone_id, d_filter)]
        result["zone_id"] = zone_id
        result["number_of_trips"] = values
        result["zone"] = self.zones.get_zone(conn, zone_id)
        return result
    
    def get_stats(self, conn, d_filter):
        records = []
        for zone_id in d_filter.get_zones():
            result = self.query_stats(conn, zone_id, d_filter)
            records.append(result)
        return records

    def serialize_rentals(self, rentals, is_arrival):
        result = []
        for rental in rentals:
            result.append(self.serialize_rental(rental, is_arrival))

        return result
        
    def serialize_rental(self, rental, is_arrival):
        data = {}
        data["system_id"] = rental[0]
        data["bike_id"] = rental[1]
        data["location"] = {}
        data["location"]["latitude"] = rental[2] 
        data["location"]["longitude"] = rental[3]
        if is_arrival:
            data["arrival_time"] = rental[4]
        else:
            data["departure_time"] = rental[4]
        return data
       


