from flask import Flask, jsonify, request
from flask.json import JSONEncoder
from datetime import date
import datetime
import psycopg2
import os
import json

import trips
import zones
import park_events
import data_filter

# Custom JSON serializer to output timestamps as ISO8601
class CustomJSONEncoder(JSONEncoder):
    def default(self, obj):
        try:
            if isinstance(obj, date):
                return obj.isoformat()
            iterable = iter(obj)
        except TypeError:
            pass
        else:
            return list(iterable)
        return JSONEncoder.default(self, obj)

class InvalidUsage(Exception):
    status_code = 400

    def __init__(self, message, status_code=None, payload=None):
        Exception.__init__(self)
        self.message = message
        if status_code is not None:
            self.status_code = status_code
        self.payload = payload

    def to_dict(self):
        rv = dict(self.payload or ())
        rv['message'] = self.message
        return rv


app = Flask(__name__)
app.json_encoder = CustomJSONEncoder

@app.errorhandler(InvalidUsage)
def handle_invalid_usage(error):
    response = jsonify(error.to_dict())
    response.status_code = error.status_code
    return response


# Initialisation
conn_str = "dbname=deelfietsdashboard"
if "dev" in os.environ:
    conn_str = "dbname=deelfietsdashboard2"

if "ip" in os.environ:
    conn_str += " host={} ".format(os.environ['ip'])
if "password" in os.environ:
    conn_str += " user=deelfietsdashboard password={}".format(os.environ['password'])


conn = psycopg2.connect(conn_str)
cur = conn.cursor()
tripAdapter = trips.Trips(conn)
zoneAdapter = zones.Zones(conn)

@app.route("/cycles")
def bike_locations(): 
  
    if "sw_lng" in request.args and "sw_lat" in request.args and "ne_lng" in request.args and "ne_lat" in request.args:
        result = get_bicycles_within_bounding_box(
            request.args.get("sw_lng"),
            request.args.get("sw_lat"),
            request.args.get("ne_lng"),
            request.args.get("ne_lat"))
    elif request.args.get('gm_code'):
        result = get_bicycles_in_municipality(request.args.get('gm_code'))
    else:
        result = get_all_bicycles()

    output = {}
    output["bicycles"] = []
    for record in result:
        output["bicycles"].append(serialize_location(record))

    conn.commit()
    return jsonify(output)

def serialize_location(result):
    data = {}
    data["last_time_position_reported"] = result[0]
    data["bike_id"] = result[1]
    data["location"] = {}
    data["location"]["latitude"] = result[2] 
    data["location"]["longitude"] = result[3]
    data["system_id"] = result[4]
    data["timestamp_end_last_trip"] = result[5]
    data["last_trip_id"] = result[6]
    return data


def get_bicycles_within_bounding_box(sw_lng, sw_lat, ne_lng, ne_lat):
    stmt = """
        SELECT last_time_imported, last_detection_bike.bike_id,
            ST_Y(location), ST_X(location), last_detection_bike.system_id, 
            end_time, trip_id
	    FROM last_detection_bike 
        LEFT JOIN last_trip_bike
        ON last_detection_bike.bike_id = last_trip_bike.bike_id
        WHERE location && ST_MakeEnvelope(%s, %s, %s, %s, 4326)
    """
    try:
        cur.execute(stmt, (sw_lng, sw_lat, ne_lng, ne_lat))
    except:
        conn.rollback()
    return cur.fetchall()

def get_bicycles_in_municipality(municipality):
    stmt = """SELECT last_time_imported, q1.bike_id,
        ST_Y(location), ST_X(location), q1.system_id, 
        end_time, trip_id 
        FROM last_detection_bike as q1
        JOIN (SELECT bike_id, sample_id
            FROM last_detection_bike as q1
            WHERE
            ST_WITHIN(location, 
                (SELECT geom 
                FROM municipalities 
                WHERE gm_code=%s
                AND geom IS NOT null 
                LIMIT 1) )) as q2
        ON q1.bike_id = q2.bike_id AND q1.sample_id = q2.sample_id
        LEFT JOIN last_trip_bike
        ON q1.bike_id = last_trip_bike.bike_id"""
    cur.execute(stmt, (municipality,))

    return cur.fetchall()

def get_all_bicycles():
    stmt = """SELECT last_time_imported, last_detection_bike.bike_id,
            ST_Y(location), ST_X(location), last_detection_bike.system_id, 
            end_time, trip_id 
            FROM last_detection_bike
            LEFT JOIN last_trip_bike
            ON last_detection_bike.bike_id = last_trip_bike.bike_id"""
    cur.execute(stmt)
    return cur.fetchall()

@app.route("/area")
def get_areas(): 
    output = {}
    if request.args.get('gm_code'):
        area = get_municipality_area(request.args.get('gm_code'))[0]
        if area:
            output["geojson"] = json.loads(area)
            output["gm_code"] = request.args.get('gm_code')

    conn.commit()
    return jsonify(output)

def get_municipality_area(municipality):
    stmt = """
        SELECT ST_AsGeoJSON(geom)
        FROM municipalities
        WHERE gm_code = %s and geom is not null"""
    cur.execute(stmt, (municipality,))
    return cur.fetchone()

@app.route("/trips")
def get_trips():
    print(request.args)
    d_filter = data_filter.DataFilter.build(request.args)

    result = {}
    result["trips"] = tripAdapter.get_trips(d_filter)
    return jsonify(result)

@app.route("/zones")
def get_zones():
    d_filter = data_filter.DataFilter.build(request.args)
    if not (d_filter.has_gmcode and d_filter.has_zone_filter):
        raise InvalidUsage("No gm_code or zone_ids.", status_code=400)

    result = {}
    result["zones"] = zoneAdapter.get_zones(d_filter)

    return jsonify(result)

@app.route("/zone/<zone_id>")
def zone():
    if request.method == 'GET':
        result = zoneAdapter.get_zones(zones)
        return jsonify(result)

@app.route("/zone", methods=['PUT', 'POST'])
def insert_zone():
    result, err = zoneAdapter.create_zone(request.data)
    if err:
        raise InvalidUsage(err, status_code=400)
    return jsonify(result), 201

parkEventsAdapter = park_events.ParkEvents(conn)

@app.route("/park_events")
def get_park_events():
    d_filter = data_filter.DataFilter.build(request.args)

    result = {}
    result["park_events"] = parkEventsAdapter.get_park_events(d_filter) 
    return jsonify(result)



