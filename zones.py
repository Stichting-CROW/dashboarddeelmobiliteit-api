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

    def create_zone(self, zone):
        cur = self.conn.cursor()
        try:
            data = json.loads(zone)
        except:
            return None, "Invalid JSON"

        stmt = """
        INSERT INTO zones
        (area, name, owner)
        VALUES
        (ST_SET_SRID(ST_GeomFromGeoJSON(%s), 4326), %s, %s)
        RETURNING zone_id
        """
        print(data)
        cur.execute(stmt, (data.get("geojson"), data.get("name"), None))

        data["zone_id"] = cur.fetchone()[0]
        self.conn.commit()
        return data, None
        

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
       


