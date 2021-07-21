import json

class PublicZoningStats():
    def __init__(self, conn):
        self.conn = conn

    def get_stats(self):
        zones = self.get_zones()
        for zone in zones:
            zone.update( {"operators": []} )
        
        zone_to_index = {k["zone_id"]: v for v, k in enumerate(zones)}
        zone_ids = [zone['zone_id'] for zone in zones]
        counts = self.query_stats(zone_ids)
        for count in counts:
            idx = zone_to_index[count[0]]
            zone = zones[idx]["operators"].append(
                {
                    "system_id": count[1],
                    "number_of_vehicles": count[2]
                }
            )
        zone_geometries = self.query_zones(zone_ids)
        for zone_geometry in zone_geometries:
            idx = zone_to_index[zone_geometry[0]]
            zone = zones[idx]["geojson"] = json.loads(zone_geometry[1])
        return zones

    def query_stats(self, zone_ids):
        cur = self.conn.cursor()
        stmt = """
            SELECT zone_id, system_id, count(*) 
            FROM park_events
            JOIN zones
            ON ST_Within(location, area)
            WHERE end_time 
            is null
            AND zone_id in %s
            GROUP by zone_id, system_id;
        """
        cur.execute(stmt, (tuple(zone_ids),))
        return cur.fetchall()

    def query_zones(self, zone_ids):
        cur = self.conn.cursor()
        stmt = """
            SELECT zone_id, ST_AsGeoJSON(area) as geometry
            FROM zones
            WHERE zone_id in %s;
        """
        cur.execute(stmt, (tuple(zone_ids),))
        return cur.fetchall()

    # For now some hardcoding to speedup the development process.
    # In the future this should be in a DB. 
    # and should be depending on the municipality you select.
    def get_zones(self):
        return [
            {
                "name": "Eind Zwarte Pad",
                "zone_id": 51184,
                "capacity": 114   
            },
            {
                "name": "Midden Zwarte Pad",
                "zone_id": 51186,
                "capacity": 84   
            },
            {
                "name": "Begin Zwarte Pad",
                "zone_id": 51185,
                "capacity": 66   
            },
            {
                "name": "Korte Zeekant",
                "zone_id": 51207,
                "capacity": 110   
            },
            {
                "name": "Strandweg (Seinpostduin)",
                "zone_id": 51200,
                "capacity": 84   
            },
            {
                "name": "Keerlus Tramlijn 11",
                "zone_id": 51199,
                "capacity": 22   
            },
            {
                "name": "Visserhavenweg",
                "zone_id": 51193,
                "capacity": 40   
            },
            {
                "name": "Biesieklette Noordelijk Havenhoofd",
                "zone_id": 51198,
                "capacity": 24   
            },
            {
                "name": "Strandslag 12",
                "zone_id": 51197,
                "capacity": 60  
            },
            {
                "name": "Biesieklette Kijkduin",
                "zone_id": 51215,
                "capacity": 40  
            },
            {
                "name": "Parking Strand",
                "zone_id": 51209 
            },
            {
                "name": "Rand servicegebied Keizerstraat",
                "zone_id": 51214  
            },
            {
                "name": "Rand servicegebied Badhuisweg",
                "zone_id": 51213
            },
            {
                "name": "Rand servicegebied Duinstraat",
                "zone_id": 51212
            },
            {
                "name": "Rand servicegebied Vissershavenweg",
                "zone_id": 51211 
            },
            {
                "name": "Den Haag Centraal",
                "zone_id": 16810
            },
            {
                "name": "nabij HS",
                "zone_id": 16776
            }
        ]
