from flask import Flask, jsonify, request, g, abort
from functools import wraps

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
import access_control
import admin_user


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
accessControl = access_control.AccessControl(conn)
adminControl = admin_user.AdminControl(conn)

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

def requires_auth(f):
    @wraps(f)
    def decorated(*args, **kwargs):
        g.acl = accessControl.retrieve_acl_user(request)
        if not g.acl:  
            abort(401)
        return f(*args, **kwargs)
    return decorated

def not_authorized(error_msg):
    data = {}
    data["error"] = error_msg
    return jsonify(data), 403

app = Flask(__name__)
app.json_encoder = CustomJSONEncoder

@app.errorhandler(InvalidUsage)
def handle_invalid_usage(error):
    response = jsonify(error.to_dict())
    response.status_code = error.status_code
    return response

@app.errorhandler(401)
def unauthorized(error):
    print(error)
    response = jsonify({'code': 401, 'message': 'You are not authorized (no token or invalid token is present).'})
    response.status_code = 401
    return response

@app.route("/cycles")
@requires_auth
def bike_locations():
    if not g.acl.is_admin():
        return not_authorized("This endpoint can only be used by administrators.")  

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
    data["timestamp"] = result[0]
    data["bike_id"] = result[1]
    data["location"] = {}
    data["location"]["latitude"] = result[2] 
    data["location"]["longitude"] = result[3]
    data["system_id"] = result[4]
    data["is_check_in"] = result[5]
    data["is_check_out"] = result[6]
    return data


def get_bicycles_within_bounding_box(sw_lng, sw_lat, ne_lng, ne_lat):
    stmt = """
        SELECT last_time_imported, last_detection_bike.bike_id,
            ST_Y(location), ST_X(location), last_detection_bike.system_id,
            is_check_in, is_check_out 
	    FROM last_detection_bike 
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
        is_check_in, is_check_out 
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
        ON q1.bike_id = q2.bike_id AND q1.sample_id = q2.sample_id"""
    cur.execute(stmt, (municipality,))

    return cur.fetchall()

def get_all_bicycles():
    stmt = """SELECT last_time_imported, last_detection_cycle.bike_id,
            ST_Y(location), ST_X(location), last_detection_cycle.system_id, 
            is_check_in, is_check_out
            FROM last_detection_cycle"""
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
@requires_auth
def get_trips():
    d_filter = data_filter.DataFilter.build(request.args)
    authorized, error = g.acl.is_authorized(d_filter)
    if not authorized:
        return not_authorized(error)

    result = {}
    result["trips"] = tripAdapter.get_trips(d_filter)
    return jsonify(result)

@app.route("/zones")
def get_zones():
    d_filter = data_filter.DataFilter.build(request.args)
    if not (d_filter.has_gmcode() or d_filter.has_zone_filter()):
        raise InvalidUsage("No gm_code or zone_ids.", status_code=400)
    
    result = {}
    if request.args.get("include_geojson") and request.args.get("include_geojson") == 'true':
        result["zones"] = zoneAdapter.get_zones(d_filter)
    else:
        result["zones"] = zoneAdapter.list_zones(d_filter)

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

@app.route("/park_events", methods=['GET'])
@requires_auth
def get_park_events():
    d_filter = data_filter.DataFilter.build(request.args)
    authorized, error = g.acl.is_authorized(d_filter)
    if not authorized:
        return not_authorized(error)

    result = {}
    result["park_events"] = parkEventsAdapter.get_park_events(d_filter) 
    return jsonify(result)

@app.route("/park_events/stats")
@requires_auth
def get_park_events_stats():
    d_filter = data_filter.DataFilter.build(request.args)
    authorized, error = g.acl.is_authorized(d_filter)
    if not authorized:
        return not_authorized(error)

    result = {}
    result["park_event_stats"] = parkEventsAdapter.get_stats(d_filter) 
    return jsonify(result)

def get_raw_gbfs(feed):
    stmt = """SELECT json
        FROM raw_gbfs
        WHERE
        feed = %s
        """
    cur.execute(stmt, (feed,))
    return cur.fetchone()[0]

# This method should also be accesible without 
@app.route("/gbfs")
def get_gbfs():
    data = {}
    if request.args.get('feed'):
        data = get_raw_gbfs(request.args.get('feed'))

    conn.commit()
    return jsonify(data)

@app.route("/admin/user/permission", methods=['GET'])
@requires_auth
def get_permission():
    if request.args.get("username") and not g.acl.is_admin:
        return not_authorized("This user is not an administrator.")
    if request.args.get("username"):
        data = accessControl.query_acl(request.args.get("username"))
    else: 
        # Default show login of user belonging to token.
        data = g.acl
    
    return jsonify(data.serialize())

@app.route("/admin/user/permission", methods=['PUT', 'POST'])
@requires_auth
def change_permission():
    if not g.acl.is_admin():
        return not_authorized("This user is not an administrator.")

    print(request.get_json())
    err = adminControl.validate(request.get_json())
    if err:
        raise InvalidUsage(err, status_code=400)
    adminControl.update(request.get_json())
   
    return jsonify(request.get_json())

# This endpoint returns the same as get_permission but add some human readable fields.
@app.route("/menu/acl", methods=['GET'])
@requires_auth
def show_human_readable_permission():
    data = g.acl
    return jsonify(data.human_readable_serialize())
