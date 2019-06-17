import json
from bson import json_util
import psycopg2.extras

class Zones():
    def __init__(self, conn):
        self.conn = conn    

    def list_zones(self, d_filter):
        cur = self.conn.cursor()
        stmt = """
            SELECT zone_id, name, owner, municipality, zone_type
            FROM zones
            WHERE municipality = %s
        """
        cur.execute(stmt, (d_filter.get_gmcode(),))
        self.conn.commit()
        return self.serialize_zones(cur.fetchall())

    def get_zone(self, zone_id):
        cur = self.conn.cursor()
        stmt = """ 
        SELECT zone_id, name, owner,
        municipality, zone_type
        FROM zones
        WHERE zone_id = %s
        """
        cur.execute(stmt, (zone_id,))
        self.conn.commit()

        if cur.rowcount > 0:
            return self.serialize_zone(cur.fetchone())


    def get_zones(self, d_filter):
        cur = self.conn.cursor()
        stmt = """ 
        SELECT zone_id, name, owner,
        municipality, zone_type, ST_AsGeoJSON(area)
        FROM zones
        WHERE (false = %s or zone_id in %s)
        AND (false = %s or municipality = %s)
        """
        cur.execute(stmt, (d_filter.has_zone_filter(), d_filter.get_zones(),
            d_filter.has_gmcode(), d_filter.get_gmcode()))
        self.conn.commit()
        
        return self.serialize_zones(cur.fetchall())

    def create_zone(self, data):
        cur = self.conn.cursor()

        if not self.check_if_zone_is_valid(data):
            return None, "Zone is outside municipality borders."

        stmt = """
        INSERT INTO zones
        (area, name, municipality)
        VALUES
        (ST_SetSRID(ST_GeomFromGeoJSON(%s), 4326), %s, %s)
        RETURNING zone_id
        """
        print(data)
        cur.execute(stmt, (json.dumps(data.get("geojson")), data.get("name"), data.get("municipality")))

        data["zone_id"] = cur.fetchone()[0]
        self.conn.commit()
        return data, None

    def check_if_zone_is_valid(self, zone_data):
        cur = self.conn.cursor()
        stmt = """  
        SELECT ST_WITHIN(
	        ST_SetSRID(ST_GeomFromGeoJSON(%s), 4326), 
            -- Add some buffer to allow drawing a little bit out of the municipality border
            (SELECT st_buffer(area, 0.02) 
            FROM zones
            WHERE municipality = %s
            AND zone_type = 'municipality'
            limit 1)
        );
        """
        cur.execute(stmt, (json.dumps(zone_data.get("geojson")), zone_data.get("municipality")))
        return cur.fetchone()[0]

    def serialize_zones(self, zones):
        result = []
        for zone in zones:
            result.append(self.serialize_zone(zone))
        return result
        
    def serialize_zone(self, zone):
        data = {}
        data["zone_id"] = zone[0]
        data["name"] = zone[1]
        data["owner"] = zone[2] 
        data["municipality"] = zone[3]
        data["zone_type"] = zone[4]
        if len(zone) > 5:
            data["geojson"] = json.loads(zone[5])
        return data
       


