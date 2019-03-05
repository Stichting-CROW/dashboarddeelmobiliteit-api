import json
from bson import json_util
import psycopg2.extras

class Zones():
    def __init__(self, conn):
        self.conn = conn

    def get_zones(self, zone_ids):
        cur = self.conn.cursor()
        stmt = """ 
        SELECT zone_id, ST_AsGeoJSON(area), name, owner
        FROM zones
        WHERE zone_id in %s
        """
        cur.execute(stmt, (zone_ids,))
        self.conn.commit()
        return self.serialize_zones(cur.fetchall())

    def create_zone(self, zone):
        cur = self.conn.cursor()
        try:
            data = json.loads(zone)
        except:
            return None, "Invalid JSON"
        geojson = json.dumps(data["geojson"])

        stmt = """
        INSERT INTO zones
        (area, name, owner)
        VALUES
        (ST_SETSRID(ST_GeomFromGeoJSON(%s), 4326), %s, %s)
        RETURNING zone_id
        """
        cur.execute(stmt, (geojson, data.get("name"), None))
        self.conn.commit()
        data["zone_id"] = cur.fetchone()[0]
        return data, None
        

    def serialize_zones(self, zones):
        result = []
        for zone in zones:
            result.append(self.serialize_zone(zone))
        return result
        
    def serialize_zone(self, trip):
        data = {}
        data["zone_id"] = trip[0]
        data["geojson"] = json.loads(trip[1])
        data["name"] = trip[2]
        data["owner"] = trip[3] 
        return data
       


