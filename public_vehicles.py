
class PublicAvailableVehicles():
    def __init__(self, conn):
        self.conn = conn

    def get_vehicles(self):
        vehicles = self.query_vehicles()
        return vehicles

    # Want to add isReserved en isDisabled here
    def query_vehicles(self):
        cur = self.conn.cursor()
        stmt = """
            SELECT system_id, 
            ST_Y(location), ST_X(location), 
	        start_time
            FROM park_events
            WHERE end_time is null;
        """
        cur.execute(stmt)
        return self.serialize_vehicles(cur.fetchall())

    def serialize_vehicles(self, vehicles):
        result = []
        for vehicle in vehicles:
            result.append(self.serialize_vehicle(vehicle))
        return result
        
    def serialize_vehicle(self, park_event):
        data = {}
        data["system_id"] = park_event[0]
        data["location"] = {}
        data["location"]["latitude"] = round(park_event[1], 6)
        data["location"]["longitude"] = round(park_event[2], 6)
        data["in_public_space_since"] = park_event[3]
        return data