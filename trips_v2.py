import json
from bson import json_util
import psycopg2.extras
import zones

class Trips():
    def __init__(self, conn):
        self.conn = conn
        self.zones = zones.Zones(conn)

    def get_trip_origins(self, d_filter):
        cur = self.conn.cursor()
        stmt = """ 
        WITH temp_a (filter_area) AS
            (
            SELECT st_union(area) 
	            FROM zones WHERE zone_id IN %s
        )

        SELECT trips.system_id, st_y(start_location), st_x(start_location),
        start_time, form_factor,
        ROUND(    
            ST_Distance(
			    ST_Transform(start_location::geometry, 3857),
			    ST_Transform(end_location::geometry, 3857)
		    ) * cosd(ST_Y(start_location)
        ) / 100) * 100 as distance_in_meters
        FROM trips CROSS JOIN temp_a
        LEFT JOIN vehicle_type
        ON trips.vehicle_type_id = vehicle_type.vehicle_type_id
        WHERE 
        start_time >= %s
        AND end_time <= %s
        AND (false = %s or 
            (ST_Within(start_location, temp_a.filter_area) OR
             ST_Within(end_location, temp_a.filter_area) ))
        AND (false = %s or trips.system_id IN %s)
        AND (false = %s or 
                (form_factor in %s or (true = %s and form_factor is null))
            ) 
        """
        cur.execute(stmt, (d_filter.get_zones(), d_filter.get_start_time(), 
            d_filter.get_end_time(), d_filter.has_zone_filter(),
            d_filter.has_operator_filter(), d_filter.get_operators(),
            d_filter.has_form_factor_filter(), d_filter.get_form_factors(),
            d_filter.include_unknown_form_factors()))
        return self.serialize_trip_events(cur.fetchall())

    def get_trip_destinations(self, d_filter):
        cur = self.conn.cursor()
        stmt = """ 
        WITH temp_a (filter_area) AS
            (
            SELECT st_union(area) 
	            FROM zones WHERE zone_id IN %s
        )

        SELECT trips.system_id, st_y(end_location), st_x(end_location), 
        end_time, form_factor,
        ROUND(    
            ST_Distance(
			    ST_Transform(start_location::geometry, 3857),
			    ST_Transform(end_location::geometry, 3857)
		    ) * cosd(ST_Y(start_location)
        ) / 100) * 100 as distance_in_meters
        FROM trips CROSS JOIN temp_a
        LEFT JOIN vehicle_type
        ON trips.vehicle_type_id = vehicle_type.vehicle_type_id
        WHERE 
        start_time >= %s
        AND end_time <= %s
        AND (false = %s or 
            (ST_Within(start_location, temp_a.filter_area) OR
             ST_Within(end_location, temp_a.filter_area) ))
        AND (false = %s or trips.system_id IN %s)
        AND (false = %s or 
                (form_factor in %s or (true = %s and form_factor is null))
            ) 
        """
        cur.execute(stmt, (d_filter.get_zones(), d_filter.get_start_time(), 
            d_filter.get_end_time(), d_filter.has_zone_filter(),
            d_filter.has_operator_filter(), d_filter.get_operators(),
            d_filter.has_form_factor_filter(), d_filter.get_form_factors(),
            d_filter.include_unknown_form_factors()))
        return self.serialize_trip_events(cur.fetchall())

    def query_stats(self, zone_id, d_filter):
        cur = self.conn.cursor()
        stmt = """WITH temp_a (filter_area) AS
            (SELECT st_union(area) 
	            FROM zones WHERE zone_id = %s)

            SELECT SUM(CASE WHEN ST_Within(start_location, temp_a.filter_area) THEN 1 ELSE 0 END), 
                SUM(CASE WHEN ST_Within(end_location, temp_a.filter_area) THEN 1 ELSE 0 END)
            FROM trips, temp_a
            WHERE 
            start_time >= %s
            AND end_time <= %s
            AND ((ST_Within(start_location, temp_a.filter_area) OR
                ST_Within(end_location, temp_a.filter_area) ))
            AND (false = %s or system_id IN %s) ;
        """
        cur.execute(stmt, (zone_id, 
            d_filter.get_start_time(), d_filter.get_end_time(),
            d_filter.has_operator_filter(), d_filter.get_operators()))

        result = {}
        result["zone_id"] = zone_id
        result["number_of_trips"] = self.get_stat_values(cur.fetchone())
        result["zone"] = self.zones.get_zone(zone_id)
        return result

    def get_stat_values(self, data):
        if not data[0]:
            return [0, 0]
        else:
            return data
        
    
    def get_stats(self, d_filter):
        records = []
        for zone_id in d_filter.get_zones():
            result = self.query_stats(zone_id, d_filter)
            records.append(result)
        return records

    def serialize_trip_events(self, trip_events):
        result = []
        for trip_event in trip_events:
            result.append(self.serialize_trip_event(trip_event))
        return result
        
    # This function can serialize an origin or a destination of a trip.
    def serialize_trip_event(self, trip):
        data = {}
        data["system_id"] = trip[0]
        data["location"] = {}
        data["location"]["latitude"] = trip[1] 
        data["location"]["longitude"] = trip[2]
        data["event_time"] = trip[3]
        data["form_factor"] = trip[4]
        data["distance_in_meters"] = trip[5]
        return data
       


