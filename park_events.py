import json
from bson import json_util
import psycopg2.extras
import zones
from datetime import datetime
from flask import g

class ParkEvents():
    def __init__(self):
        self.zones = zones.Zones()

    def get_private_park_events(self, conn, d_filter):
        rows = self.get_park_events(conn, d_filter)
        return self.serialize_park_events(rows)

    def get_public_park_events(self, conn, d_filter):
        d_filter.timestamp = datetime.now()
        rows = self.get_park_events(conn, d_filter)
        return self.serialize_public_park_events(rows)

    def get_park_events(self, conn, d_filter):
        cur = conn.cursor()
        stmt = """ 
        SELECT park_events.system_id, bike_id, 
	        ST_Y(location), ST_X(location), 
	        start_time, end_time, form_factor
        FROM park_events
        LEFT JOIN vehicle_type 
        ON park_events.vehicle_type_id = vehicle_type.vehicle_type_id
        WHERE start_time < %s 
            AND (end_time > %s OR end_time is null)
        AND (false = %s or ST_WITHIN(location, 
	    (SELECT st_union(area) 
	    FROM zones WHERE zone_id IN %s)))
        AND (false = %s or park_events.system_id IN %s)
        AND (false = %s or 
                (form_factor in %s or (true = %s and form_factor is null))
            ) ;
        """
        cur.execute(stmt, (d_filter.get_timestamp(), d_filter.get_timestamp(), 
            d_filter.has_zone_filter(), d_filter.get_zones(),
            d_filter.has_operator_filter(), d_filter.get_operators(),
            d_filter.has_form_factor_filter(), d_filter.get_form_factors(),
            d_filter.include_unknown_form_factors()))
        return cur.fetchall()

    def get_stats_per_zone(self, conn, d_filter, zone_id):
        cur = conn.cursor()
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

    def get_stats(self, conn, d_filter):
        records = []
        for zone_id in d_filter.get_zones():
            result = self.get_stats_per_zone(conn, d_filter, zone_id)
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
        data["form_factor"] = park_event[6]
        return data

    def serialize_public_park_events(self, park_events):
        result = []
        for park_event in park_events:
            result.append(self.serialize_public_park_event(park_event))

        return result
        
    def serialize_public_park_event(self, park_event):
        data = {}
        data["system_id"] = park_event[0]
        data["location"] = {}
        data["location"]["latitude"] = park_event[2] 
        data["location"]["longitude"] = park_event[3]
        data["form_factor"] = park_event[6]
        return data

    # Same data as private endpoint, with the difference that you only can get the data for this moment
    def get_public_park_event_stats(self, conn, d_filter):
        d_filter.timestamp = datetime.now()
        return self.get_park_events_stats(conn, d_filter)

    def get_park_event_stats(self, conn, d_filter):
        cur = conn.cursor()
        stmt = """WITH park_event_stats AS 
            (SELECT zone_id,
            CASE 
                WHEN datef < '1 HOUR' THEN 0
                WHEN datef >= '1 HOUR' and datef < '1 DAY' THEN 1
                WHEN datef >= '1 DAY' and datef < '4 DAYS' THEN 2
                ELSE 3
            END as bucket,
            SUM(sum_bikes) as number_of_park_events
            FROM
                (SELECT date_trunc('hour', %s - start_time) as datef, zone_id,
                    count(1) as sum_bikes
                FROM (
                        SELECT start_time, park_events.system_id, zones.zone_id 
                        FROM park_events
                        JOIN zones
                        ON ST_WITHIN(location, area)
                        LEFT JOIN vehicle_type 
                        USING(vehicle_type_id)
                        WHERE start_time < %s
                        AND (end_time > %s  or end_time is null)
                        AND (false = %s or park_events.system_id IN %s)
                        AND (false = %s or 
                                (form_factor in %s or (true = %s and form_factor is null))
                            )
                        AND zone_id in %s
                    ) AS q1
                    GROUP BY datef, zone_id, system_id) q1
                GROUP BY zone_id, bucket
            ORDER BY zone_id, bucket),
            grouped_park_event_stats
            AS 
                (SELECT zone_id, json_object_agg(bucket, number_of_park_events) as stats
                FROM park_event_stats
                GROUP BY zone_id)

            SELECT zone_id, name, municipality, zone_type, stats
            FROM zones
            LEFT JOIN grouped_park_event_stats
            USING(zone_id)
            WHERE zones.zone_id in %s;
        """
        zone_ids = tuple([zone_id for zone_id in  d_filter.get_zones()])
        cur.execute(stmt, (d_filter.get_timestamp(), d_filter.get_timestamp(), d_filter.get_timestamp(),
            d_filter.has_operator_filter(), d_filter.get_operators(), 
            d_filter.has_form_factor_filter(), d_filter.get_form_factors(), 
            d_filter.include_unknown_form_factors(), zone_ids, zone_ids))
        zones = cur.fetchall()
        result_zones = []
        for zone in zones:
            result_zones.append(self.serialize_park_event_stat(zone))
        return result_zones

    def serialize_park_event_stat(self, zone):
        data = {}
        data["zone_id"] = zone[0]
        data["name"] = zone[1] 
        data["municipality"] = zone[2]
        data["zone_type"] = zone[3]
        data["stats"] = self.serialize_park_event_stat_details(zone[4])
        return data      

    def serialize_park_event_stat_details(self, details):
        data = []
        if details == None:
            return [0, 0, 0, 0]
        for x in range(4):
            if str(x) in details:
                data.append(details[str(x)])
            else:
                data.append(0)
        return data

    # parkeertelling :: Do a parkeertelling
    #
    # Example input:
    # 
    # "name": "Arnhem Ketelstraat oneven zijde",
    # "timestamp": "2022-10-24T00:00:00",
    # "geojson": {
    #   "type": "Polygon", 
    #   "coordinates":  [
    #     [
    #       [5.90802,51.98173],
    #       [5.90808,51.98171],
    #       [5.90924,51.98199],
    #       [5.90921,51.98202],
    #       [5.90802,51.98173]
    #     ]
    #   ]
    # }
    #
    # Example cURL call:
    # curl -XPOST -H "Content-type: application/json" -d '{ 
    # "timestamp": "2022-10-24T00:00:00",
    # "geojson": {
    #   "type": "Polygon", 
    #   "coordinates":  [
    #     [
    #       [5.90802,51.98173],
    #       [5.90808,51.98171],
    #       [5.90924,51.98199],
    #       [5.90921,51.98202],
    #       [5.90802,51.98173]
    #     ]
    #   ]
    # }' 'https://api.deelfietsdashboard.nl/dashboard-api/parkeertelling'
    #
    def parkeertelling(self, conn, d_filter):
        conn = get_conn()
        cur = conn.cursor()

        stmt = """SELECT
                form_factor,
                COUNT(bike_id) as number_of_parked_vehicles
            FROM park_events
            
            LEFT JOIN vehicle_type 
            ON park_events.vehicle_type_id = vehicle_type.vehicle_type_id
            
            WHERE   start_time < %s 
                AND (end_time > %s OR end_time is null)
                AND ST_WITHIN(
                    location,
                    %s
                )
            GROUP BY form_factor;
        """

        cur.execute(stmt, (
            d_filter.get_timestamp(),# Start time
            d_filter.get_timestamp(),# End time
            d_filter.get_geojson()   # GeoJSON area to search in
        ));

        return cur.fetchall()
