import json
from bson import json_util
import psycopg2.extras
import zones
from flask import g

class ParkEvents():
    def __init__(self, conn):
        self.conn = conn
        self.zones = zones.Zones(conn)

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
	    FROM zones WHERE zone_id IN %s)))
        AND (false = %s or system_id IN %s);
        """
        cur.execute(stmt, (d_filter.get_timestamp(), d_filter.get_timestamp(), 
            d_filter.has_zone_filter(), d_filter.get_zones(),
            d_filter.has_operator_filter(), d_filter.get_operators()))
        return self.serialize_park_events(cur.fetchall())

    def get_stats_per_zone(self, d_filter, zone_id):
        cur = self.conn.cursor()
        stmt = """ 
        SELECT 
            CASE 
                WHEN datef < '1 DAY' THEN 0
                WHEN datef >= '1 DAY' and datef < '2 DAYS' THEN 1
                WHEN datef >= '2 DAYS' and datef < '3 DAYS' THEN 2
                WHEN datef >= '3 DAYS' and datef < '5 DAYS' THEN 3
                WHEN datef >= '5 DAYS' and datef < '7 DAYS' THEN 4
                ELSE 5
            END as bucket,
            SUM(sum_bikes) as number_of_park_events
        FROM
        (SELECT date_trunc('day', %s - start_time) as datef, 
            count(1) as sum_bikes
        FROM (
            SELECT * 
            FROM park_events
            WHERE start_time < %s 
            AND (end_time > %s or end_time is null)
            AND ST_WITHIN(location, 
                (SELECT area
                FROM zones 
                WHERE zone_id = %s))
                AND (false = %s or system_id IN %s)) AS q1
            GROUP BY datef) q1
        GROUP BY bucket
        ORDER BY bucket;
        """
        cur.execute(stmt, (d_filter.get_timestamp(), d_filter.get_timestamp(), 
            d_filter.get_timestamp(), 
            zone_id, d_filter.has_operator_filter(), d_filter.get_operators()))

        result = {}
        result["zone_id"] = zone_id
        result["number_of_bicycles_parked_for"] = self.extract_stat(cur.fetchall())
        result["zone"] = self.zones.get_zone(zone_id)
        return result

    # Fill array with data.
    def extract_stat(self, records):
        result = [0] * 6
        for record in records:
            result[record[0]] = int(record[1])
        return result




    def get_stats(self, d_filter):
        records = []
        for zone_id in d_filter.get_zones():
            result = self.get_stats_per_zone(d_filter, zone_id)
            records.append(result)
        return records


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
       


