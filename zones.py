import json
from bson import json_util
import psycopg2.extras

class Zones():
    def list_zones(self, conn, d_filter, include_custom_zones=True):
        cur = conn.cursor()
        stmt = """
            SELECT zone_id, zones.name, owner, municipality, zone_type
            FROM zones
            LEFT JOIN geographies
            USING (zone_id)
            WHERE municipality = %s
            AND (true = %s or zone_type != 'custom')
            AND retire_date is null;
        """
        cur.execute(stmt, (d_filter.get_gmcode(), include_custom_zones))
        conn.commit()
        return self.serialize_zones(cur.fetchall())

    def get_zone(self, conn, zone_id):
        cur = conn.cursor()
        stmt = """ 
        SELECT zone_id, name, owner,
        municipality, zone_type
        FROM zones
        WHERE zone_id = %s
        """
        cur.execute(stmt, (zone_id,))
        conn.commit()

        if cur.rowcount > 0:
            return self.serialize_zone(cur.fetchone())


    def get_zones(self, conn, d_filter):
        cur = conn.cursor()
        stmt = """ 
        SELECT zone_id, name, owner,
        municipality, zone_type, ST_AsGeoJSON(area)
        FROM zones
        WHERE (false = %s or zone_id in %s)
        AND (false = %s or municipality = %s)
        """
        cur.execute(stmt, (d_filter.has_zone_filter(), d_filter.get_zones(),
            d_filter.has_gmcode(), d_filter.get_gmcode()))
        conn.commit()
        
        return self.serialize_zones(cur.fetchall())

    def create_zone(self, conn, data):
        cur = conn.cursor()

        if not self.check_if_zone_is_valid(data):
            return None, "Zone is outside municipality borders."

        stmt = """
        INSERT INTO zones
        (area, name, municipality, zone_type)
        VALUES
        (ST_SetSRID(ST_GeomFromGeoJSON(%s), 4326), %s, %s, 'custom')
        RETURNING zone_id
        """
        cur.execute(stmt, (json.dumps(data.get("geojson")), data.get("name"), data.get("municipality")))
        data["zone_id"] = cur.fetchone()[0]
        conn.commit()
        return data, None

    def delete_zone(self, conn, zone_id):
        cur = conn.cursor()

        stmt = """
        DELETE 
        FROM ZONES
        WHERE zone_id = %s
        AND zone_type = 'custom'
        RETURNING *
        """
        cur.execute(stmt, (zone_id,))
        conn.commit()
        succesful = (len(cur.fetchall()) > 0)
        cur.close()

        return succesful


    def check_if_zone_is_valid(self, conn, zone_data):
        cur = conn.cursor()
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

    def get_municipality_based_on_latlng(self, conn, latitude, longitude):
        stmt = """
            SELECT zone_id, name, owner, municipality, zone_type
            FROM zones 
            WHERE zone_type = 'municipality' 
            AND ST_Intersects(zones.area, ST_SetSRID(ST_POINT(%s, %s), 4326));
        """
        cur = conn.cursor()
        cur.execute(stmt, (longitude, latitude))
        res = cur.fetchone()
        if res:
            return self.serialize_zone(res)

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
