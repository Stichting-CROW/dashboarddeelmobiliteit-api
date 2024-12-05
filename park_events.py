import json
from bson import json_util
import psycopg2.extras
import zones
from datetime import datetime, timedelta
from flask import g

class ParkEvents():
    def __init__(self):
        self.zones = zones.Zones()

    def get_private_park_events(self, conn, d_filter):
        if d_filter.get_timestamp() <  datetime.now() - timedelta(hours=36):
            rows = self.get_park_events_long_term(conn, d_filter)
            return self.serialize_park_events(rows)
        rows = self.get_park_events_short_term(conn, d_filter)
        return self.serialize_park_events(rows)
    
    def get_public_park_events(self, conn, d_filter):
        d_filter.timestamp = datetime.now()
        rows = self.get_park_events_short_term(conn, d_filter)
        return self.serialize_public_park_events(rows)

    def get_park_events_long_term(self, conn, d_filter):
        cur = conn.cursor()
        stmt = """
        WITH relevant_park_ids AS (
            SELECT UNNEST(park_event_ids) as park_event_id
            FROM park_event_on_date
            WHERE on_date = %(timestamp)s::date
        )
        SELECT
            park_events.system_id, 
            bike_id, 
	        ST_Y(location), ST_X(location), 
	        start_time, 
            end_time, 
            form_factor
        FROM park_events
        LEFT JOIN vehicle_type 
        ON park_events.vehicle_type_id = vehicle_type.vehicle_type_id
        JOIN relevant_park_ids
        USING(park_event_id)
        WHERE
        start_time < %(timestamp)s
        AND (end_time > %(timestamp)s OR end_time is null)
        AND
        (
            false = %(has_zone_filter)s 
            OR ST_WITHIN(location, 
                (
                    SELECT ST_UNION(area) 
	                FROM zones 
                    WHERE zone_id IN %(zone_ids)s
                )
            )
        )
        AND (
            false = %(has_operator_filter)s 
            OR park_events.system_id IN %(system_ids)s
        )
        AND (
            false = %(has_form_factor_filter)s 
            OR (
                form_factor in %(form_factors)s 
                OR (
                    true = %(include_unknown_form_factors)s 
                    AND form_factor is null
                )
            )
        );
        """
        cur.execute(stmt, {
            "timestamp": d_filter.get_timestamp(),
            "has_zone_filter": d_filter.has_zone_filter(),
            "zone_ids": d_filter.get_zones(),
            "has_operator_filter": d_filter.has_operator_filter(),
            "system_ids": d_filter.get_operators(),
            "has_form_factor_filter": d_filter.has_form_factor_filter(),
            "form_factors": d_filter.get_form_factors(),
            "include_unknown_form_factors": d_filter.include_unknown_form_factors()
            }
        )
        return cur.fetchall()
    
    def get_park_events_short_term(self, conn, d_filter):
        cur = conn.cursor()
        stmt = """
        SELECT
            park_events.system_id, 
            bike_id, 
	        ST_Y(location), ST_X(location), 
	        start_time, 
            end_time, 
            form_factor
        FROM park_events
        LEFT JOIN vehicle_type 
        ON park_events.vehicle_type_id = vehicle_type.vehicle_type_id
        WHERE
        start_time < %(timestamp)s
        AND (end_time > %(timestamp)s OR end_time is null)
        AND
        (
            false = %(has_zone_filter)s 
            OR ST_WITHIN(location, 
                (
                    SELECT ST_UNION(area) 
	                FROM zones 
                    WHERE zone_id IN %(zone_ids)s
                )
            )
        )
        AND (
            false = %(has_operator_filter)s 
            OR park_events.system_id IN %(system_ids)s
        )
        AND (
            false = %(has_form_factor_filter)s 
            OR (
                form_factor in %(form_factors)s 
                OR (
                    true = %(include_unknown_form_factors)s 
                    AND form_factor is null
                )
            )
        );
        """
        cur.execute(stmt, {
            "timestamp": d_filter.get_timestamp(),
            "has_zone_filter": d_filter.has_zone_filter(),
            "zone_ids": d_filter.get_zones(),
            "has_operator_filter": d_filter.has_operator_filter(),
            "system_ids": d_filter.get_operators(),
            "has_form_factor_filter": d_filter.has_form_factor_filter(),
            "form_factors": d_filter.get_form_factors(),
            "include_unknown_form_factors": d_filter.include_unknown_form_factors()
            }
        )
        return cur.fetchall()

    # Fill array with data.
    def extract_stat(self, records):
        result = [0] * 6
        for record in records:
            result[record[0]] = int(record[1])
        return result


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
    
    def get_park_event_stats(self, conn, d_filter):
        if d_filter.get_timestamp() <  datetime.now() - timedelta(hours=36):
            return self.get_park_event_stats_long_term(conn, d_filter)
        return self.get_park_event_stats_short_term(conn, d_filter)

    def get_park_event_stats_short_term(self, conn, d_filter):
        cur = conn.cursor()
        stmt = """WITH park_event_stats AS 
            (SELECT zone_id,
            CASE 
                WHEN datef < '2 DAYS' THEN 0
                WHEN datef >= '2 DAYS' and datef < '4 DAY' THEN 1
                WHEN datef >= '4 DAYS' and datef < '7 DAYS' THEN 2
                WHEN datef >= '7 DAYS' and datef < '14 DAYS' THEN 3
                ELSE 4
            END as bucket,
            SUM(sum_bikes) as number_of_park_events
            FROM
                (SELECT date_trunc('hour', %(timestamp)s - start_time) as datef, zone_id,
                    count(1) as sum_bikes
                FROM (
                        SELECT start_time, park_events.system_id, zones.zone_id 
                        FROM park_events
                        JOIN zones
                        ON ST_WITHIN(location, area)
                        LEFT JOIN vehicle_type 
                        USING(vehicle_type_id)
                        WHERE 
                        start_time < %(timestamp)s
                        AND (end_time > %(timestamp)s OR end_time is null)
                        AND (
                            false = %(has_operator_filter)s 
                            OR park_events.system_id IN %(system_ids)s
                        )
                        AND (
                            false = %(has_form_factor_filter)s 
                            OR (
                                form_factor in %(form_factors)s 
                                OR (
                                    true = %(include_unknown_form_factors)s 
                                    AND form_factor is null
                                )
                            )
                        )
                        AND zone_id in %(zone_ids)s
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
            WHERE zones.zone_id in %(zone_ids)s;
        """
        zone_ids = tuple([zone_id for zone_id in  d_filter.get_zones()])
        cur.execute(stmt, {
            "timestamp": d_filter.get_timestamp(),
            "zone_ids": zone_ids,
            "has_operator_filter": d_filter.has_operator_filter(),
            "system_ids": d_filter.get_operators(),
            "has_form_factor_filter": d_filter.has_form_factor_filter(),
            "form_factors": d_filter.get_form_factors(),
            "include_unknown_form_factors": d_filter.include_unknown_form_factors()
            }
        )
        rows = cur.fetchall()
        result_zones = []
        for zone in rows:
            result_zones.append(self.serialize_park_event_stat(zone))
        return result_zones
    
    def get_park_event_stats_long_term(self, conn, d_filter):
        cur = conn.cursor()
        stmt = """
        WITH relevant_park_ids AS (
            SELECT UNNEST(park_event_ids) as park_event_id
            FROM park_event_on_date
            WHERE on_date = %(timestamp)s::date
        ),
        park_event_stats AS 
            (SELECT zone_id,
            CASE 
                WHEN datef < '2 DAYS' THEN 0
                WHEN datef >= '2 DAYS' and datef < '4 DAY' THEN 1
                WHEN datef >= '4 DAYS' and datef < '7 DAYS' THEN 2
                WHEN datef >= '7 DAYS' and datef < '14 DAYS' THEN 3
                ELSE 4
            END as bucket,
            SUM(sum_bikes) as number_of_park_events
            FROM
                (SELECT date_trunc('hour', %(timestamp)s - start_time) as datef, zone_id,
                    count(1) as sum_bikes
                FROM (
                        SELECT start_time, park_events.system_id, zones.zone_id 
                        FROM park_events
                        JOIN zones
                        ON ST_WITHIN(location, area)
                        LEFT JOIN vehicle_type 
                        USING(vehicle_type_id)
                        JOIN relevant_park_ids
                        USING(park_event_id)
                        WHERE
                        start_time < %(timestamp)s
                        AND (end_time > %(timestamp)s OR end_time is null)
                        AND (
                            false = %(has_operator_filter)s 
                            OR park_events.system_id IN %(system_ids)s
                        )
                        AND (
                            false = %(has_form_factor_filter)s 
                            OR (
                                form_factor in %(form_factors)s 
                                OR (
                                    true = %(include_unknown_form_factors)s 
                                    AND form_factor is null
                                )
                            )
                        )
                        AND zone_id in %(zone_ids)s
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
            WHERE zones.zone_id in %(zone_ids)s;
        """
        zone_ids = tuple([zone_id for zone_id in  d_filter.get_zones()])
        cur.execute(stmt, {
            "timestamp": d_filter.get_timestamp(),
            "zone_ids": zone_ids,
            "has_operator_filter": d_filter.has_operator_filter(),
            "system_ids": d_filter.get_operators(),
            "has_form_factor_filter": d_filter.has_form_factor_filter(),
            "form_factors": d_filter.get_form_factors(),
            "include_unknown_form_factors": d_filter.include_unknown_form_factors()
            }
        )
        rows = cur.fetchall()
        result_zones = []
        for zone in rows:
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
            return [0, 0, 0, 0, 0]
        for x in range(5):
            if str(x) in details:
                data.append(details[str(x)])
            else:
                data.append(0)
        return data

    # parkeertelling :: Do a parkeertelling
    #
    # Example input:
    # 
    # { 
    #   "name": "Arnhem Ketelstraat oneven zijde",
    #   "timestamp": "2022-10-24T00:00:00Z",
    #   "geojson": {
    #     "type": "Polygon",
    #     "coordinates":  [
    #       [
    #         [5.90802,51.98173],
    #         [5.90808,51.98171],
    #         [5.90924,51.98199],
    #         [5.90921,51.98202],
    #         [5.90802,51.98173]
    #       ]
    #     ]
    #   }
    # }
    #
    # Example cURL call for all of NL:
    #     curl -XPOST -H "Content-type: application/json" -d '{"timestamp": "2023-09-19T00:00:00Z", "geojson": {"type": "Polygon", "coordinates": [[[1.882351, 50.649545], [7.023702, 49.333254], [8.108420, 53.729841], [2.235547, 53.721598]]]}}' 'http://127.0.0.1:5000/parkeertelling'
    #     curl -XPOST -H "Content-type: application/json" -d '{"timestamp": "2023-09-19T00:00:00Z", "geojson": {"type": "Polygon", "coordinates": [[[1.882351, 50.649545], [7.023702, 49.333254], [8.108420, 53.729841], [2.235547, 53.721598]]]}}' 'https://api.deelfietsdashboard.nl/dashboard-api/parkeertelling?apikey=X'
    #
    def parkeertelling(self, conn, d_filter):
        if d_filter.get_timestamp() <  datetime.now() - timedelta(hours=36):
            return self.long_term_parkeertelling(conn, d_filter)
        return self.short_term_parkeertelling(conn, d_filter)
    
    def short_term_parkeertelling(self, conn, d_filter):
        cur = conn.cursor()

        stmt = """
            SELECT
            form_factor,
            COUNT(bike_id) as number_of_parked_vehicles
            FROM park_events
            LEFT JOIN vehicle_type 
            ON park_events.vehicle_type_id = vehicle_type.vehicle_type_id
            WHERE 
            start_time < %(timestamp)s
            AND (end_time > %(timestamp)s OR end_time is null)
            AND ST_WITHIN(
                location,
                ST_SetSRID(ST_GeomFromGeoJSON(%(geojson)s), 4326)
            )
            GROUP BY form_factor;
        """

        cur.execute(stmt, {
            "timestamp": d_filter.get_timestamp(),
            "geojson": json.dumps(d_filter.get_geojson())
        })

        return cur.fetchall()

    def long_term_parkeertelling(self, conn, d_filter):
        cur = conn.cursor()

        stmt = """
            WITH relevant_park_ids AS (
                SELECT UNNEST(park_event_ids) as park_event_id
                FROM park_event_on_date
                WHERE on_date = %(timestamp)s::date
            )
            SELECT
            form_factor,
            COUNT(bike_id) as number_of_parked_vehicles
            FROM park_events
            LEFT JOIN vehicle_type 
            ON park_events.vehicle_type_id = vehicle_type.vehicle_type_id
            JOIN relevant_park_ids
            USING(park_event_id)
            WHERE 
            start_time < %(timestamp)s
            AND (end_time > %(timestamp)s OR end_time is null)
            AND ST_WITHIN(
                location,
                ST_SetSRID(ST_GeomFromGeoJSON(%(geojson)s), 4326)
            )
            GROUP BY form_factor;
        """

        cur.execute(stmt, {
            "timestamp": d_filter.get_timestamp(),
            "geojson": json.dumps(d_filter.get_geojson())
        })

        return cur.fetchall()

