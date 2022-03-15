from flask import Flask, jsonify, request, g, abort, send_file, after_this_request, send_from_directory
from functools import wraps

from flask.json import JSONEncoder
from datetime import date
import datetime
import psycopg2
import os
import json
import io
import shutil
import time

import trips
import trips_v2
import zones
import park_events
import data_filter
import access_control
import admin_user
import stats_over_time
import rentals
import report.generate_xlsx
import export_raw_data.export_to_zip
import public_zoning_stats
import audit_log
import stats_active_users
import stats_aggregated_availability
import stats_aggregated_rentals

# Initialisation
conn_str = "dbname=deelfietsdashboard"
if "dev" in os.environ:
    conn_str = "dbname=deelfietsdashboard4"

if "ip" in os.environ:
    conn_str += " host={} ".format(os.environ['ip'])
if "password" in os.environ:
    conn_str += " user=deelfietsdashboard password={}".format(os.environ['password'])


conn = psycopg2.connect(conn_str)
cur = conn.cursor()
tripAdapter = trips.Trips(conn)
tripAdapterV2 = trips_v2.Trips(conn)
zoneAdapter = zones.Zones(conn)
rentalAdapter = rentals.Rentals(conn)
defaultAccessControl = access_control.DefaultACL()
accessControl = access_control.AccessControl(conn)
adminControl = admin_user.AdminControl(conn)
statsOvertime = stats_over_time.StatsOverTime(conn)
statsAggregatedAvailability = stats_aggregated_availability.AggregatedStatsAvailability(conn)
statsAggregatedRentals = stats_aggregated_rentals.AggregatedStatsRentals(conn)

# Custom JSON serializer to output timestamps as ISO8601
class CustomJSONEncoder(JSONEncoder):
    def default(self, obj):
        try:
            if isinstance(obj, date):
                return obj.isoformat() + "Z"
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
    conn.commit()
    return jsonify(result)

@app.route("/trips/stats")
@requires_auth
def get_trips_stats():
    d_filter = data_filter.DataFilter.build(request.args)
    authorized, error = g.acl.is_authorized(d_filter)
    if not authorized:
        return not_authorized(error)

    if not d_filter.get_start_time():
        raise InvalidUsage("No start_time specified", status_code=400)
    if not d_filter.get_end_time():
        raise InvalidUsage("No end_time specified", status_code=400)

    result = {}
    result["trip_stats"] = tripAdapter.get_stats(d_filter)
    conn.commit()
    return jsonify(result)


@app.route("/v2/trips/origins")
@requires_auth
def get_trips_origins():
    d_filter = data_filter.DataFilter.build(request.args)
    authorized, error = g.acl.is_authorized(d_filter)
    if not authorized:
        return not_authorized(error)

    result = {}
    result["trip_origins"] = tripAdapterV2.get_trip_origins(d_filter)
    conn.commit()
    return jsonify(result)


@app.route("/v2/trips/destinations")
@requires_auth
def get_trips_destinations():
    print("start")
    print(time.time())
    d_filter = data_filter.DataFilter.build(request.args)
    authorized, error = g.acl.is_authorized(d_filter)
    if not authorized:
        return not_authorized(error)
    print(time.time())
    result = {}
    result["trip_destinations"] = tripAdapterV2.get_trip_destinations(d_filter)
    print(time.time())
    conn.commit()
    print("end")
    print(time.time())
    return jsonify(result)

@app.route("/rentals")
@requires_auth
def get_rentals():
    d_filter = data_filter.DataFilter.build(request.args)
    authorized, error = g.acl.is_authorized(d_filter)
    if not authorized:
        return not_authorized(error)

    result = {}
    result["start_rentals"] = rentalAdapter.get_start_trips(d_filter)
    result["end_rentals"] = rentalAdapter.get_end_trips(d_filter)
    conn.commit()
    return jsonify(result)

@app.route("/rentals/stats")
@requires_auth
def get_rentals_stats():
    d_filter = data_filter.DataFilter.build(request.args)
    authorized, error = g.acl.is_authorized(d_filter)
    if not authorized:
        return not_authorized(error)

    if not d_filter.get_start_time():
        raise InvalidUsage("No start_time specified", status_code=400)
    if not d_filter.get_end_time():
        raise InvalidUsage("No end_time specified", status_code=400)

    result = {}
    result["rental_stats"] = rentalAdapter.get_stats(d_filter)
    conn.commit()
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

    conn.commit()
    return jsonify(result)

@app.route("/zone/<zone_id>", methods=['DELETE'])
@requires_auth
def zone(zone_id):
    d_filter = data_filter.DataFilter()
    d_filter.add_zone(zone_id)
    authorized, error = g.acl.is_authorized(d_filter)
    if not authorized:
        return not_authorized(error)

    deleted = zoneAdapter.delete_zone(zone_id)
    conn.commit()
    return jsonify({"deleted": deleted})

@app.route("/zone", methods=['PUT', 'POST'])
@requires_auth
def insert_zone():
    try:
        zone_data = json.loads(request.data)
    except:
        raise InvalidUsage("invalid JSON", status_code=400)
    
    if not "municipality" in zone_data:
        return not_authorized("No field 'municipality' in JSON")

    authorized, error = g.acl.check_municipality_code(zone_data["municipality"])
    if not authorized:
        return not_authorized(error)


    result, err = zoneAdapter.create_zone(zone_data)
    if err:
        raise InvalidUsage(err, status_code=400)
    return jsonify(result), 201

publicZonesAdapter = public_zoning_stats.PublicZoningStats(conn)
# MVP endpoint to retrieve public information of zones.
@app.route("/public/zones", methods=['GET'])
def get_occupancy_zones():
    result = publicZonesAdapter.get_stats()
    return jsonify(result)


@app.route("/public/vehicles_in_public_space", methods=['GET'])
def get_vehicles_in_public_space():
    d_filter = data_filter.DataFilter.build(request.args)
    
    result = {}
    result["vehicles_in_public_space"] = parkEventsAdapter.get_public_park_events(d_filter) 
    return jsonify(result)



@app.route("/public/filters", methods=['GET'])
def get_filters():
    d_filter = data_filter.DataFilter.build(request.args)
    
    result = {}
    result["filter_values"] = defaultAccessControl.serialize(conn)
    print(d_filter.has_gmcode())
    if d_filter.has_gmcode():
        result["filter_values"]["zones"] = zoneAdapter.list_zones(d_filter, include_custom_zones=False)
    return jsonify(result)


parkEventsAdapter = park_events.ParkEvents(conn)
@app.route("/park_events", methods=['GET'])
@requires_auth
def get_park_events():
    d_filter = data_filter.DataFilter.build(request.args)
    authorized, error = g.acl.is_authorized(d_filter)
    if not authorized:
        return not_authorized(error)

    result = {}
    result["park_events"] = parkEventsAdapter.get_private_park_events(d_filter) 
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

@app.route("/v2/park_events/stats")
@requires_auth
def get_park_events_stats_v2():
    d_filter = data_filter.DataFilter.build(request.args)
    authorized, error = g.acl.is_authorized(d_filter)
    if not authorized:
        return not_authorized(error)

    result = {}
    result["park_event_stats"] = parkEventsAdapter.get_park_event_stats(d_filter) 
    return jsonify(result)


# In theory it's possible to retreive data from custom zones. That is not really a problem but can be fixed in the future.
@app.route("/public/park_events/stats")
def get_park_events_stats_v2():
    d_filter = data_filter.DataFilter.build(request.args)

    result = {}
    result["park_event_stats"] = parkEventsAdapter.get_public_park_event_stats(d_filter) 
    return jsonify(result)

@app.route("/stats/available_bikes")
@requires_auth
def get_available_bicycles():
    d_filter = data_filter.DataFilter.build(request.args)
    authorized, error = g.acl.is_authorized(d_filter)
    if not authorized:
        return not_authorized(error)

    if not d_filter.get_start_time():
        raise InvalidUsage("No start_time specified", status_code=400)
    if not d_filter.get_end_time():
        raise InvalidUsage("No end_time specified", status_code=400)
    
    result = {}
    result["available_bikes"] = statsOvertime.query_stats(d_filter)
    return jsonify(result)

@app.route("/stats/generate_report")
@requires_auth
def get_report():
    d_filter = data_filter.DataFilter.build(request.args)
    if not d_filter.has_gmcode():
        raise InvalidUsage("No municipality specified", status_code=400)
    if not d_filter.get_start_time():
        raise InvalidUsage("No start_time specified", status_code=400)
    if not d_filter.get_end_time():
        raise InvalidUsage("No end_time specified", status_code=400)
   
    authorized, error = g.acl.check_municipality_code(d_filter.get_gmcode())
    if not authorized:
        return not_authorized(error)
    authorized, error = g.acl.check_operators(d_filter)
    if not authorized:
        return not_authorized(error)
 
    raw_data, file_name = report.generate_xlsx.generate_report(conn, d_filter)
    return send_file(io.BytesIO(raw_data),
                     attachment_filename=file_name + ".xlsx",
                     mimetype='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
                     cache_timeout=0,
                     as_attachment=True)

@app.route("/raw_data")
@requires_auth
def get_raw_data():
    d_filter = data_filter.DataFilter.build(request.args)
    if not d_filter.get_start_time():
        raise InvalidUsage("No start_time specified", status_code=400)
    if not d_filter.get_end_time():
        raise InvalidUsage("No end_time specified", status_code=400)
    
    # load filters
    result = d_filter.add_filters_based_on_acl(g.acl)
    if result != None:
        return not_authorized(result) 

    # check if all authorizations are matched.
    authorized, error = g.acl.is_authorized_for_raw_data(d_filter)
    if not authorized:
        return not_authorized(error)

    audit_log.log_request(conn, g.acl.username, request.full_path, d_filter)
    export_dir = export_raw_data.export_to_zip.generate_zip(conn, d_filter)
    @after_this_request
    def remove_file(response):
        try:
            shutil.rmtree(export_dir)
        except OSError as e:
            print("Error: %s : %s" % (export_dir, e.strerror))
        return response
    return send_from_directory(export_dir,
        filename="export.zip",
        attachment_filename="export_deelfietsdashboard.zip",
        as_attachment=True)


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

@app.route("/admin/user/create", methods=['PUT'])
@requires_auth
def create_user():
    if not g.acl.is_admin():
        return not_authorized("This user is not an administrator.")

    res, err = adminControl.create_user(request.get_json())    
    if not res:
        raise InvalidUsage(err, status_code=400)

    print(request.get_json())
    return jsonify(res)

@app.route("/admin/user/list", methods=['GET'])
@requires_auth
def list_user():
    if not g.acl.is_admin():
        return not_authorized("This user is not an administrator.")

    res = map(lambda acl: acl.serialize(), adminControl.list_users())

    return jsonify(res)

@app.route("/admin/user/delete", methods=['DELETE'])
@requires_auth
def delete_user():
    if not g.acl.is_admin():
        return not_authorized("This user is not an administrator.")

    username = request.args.get('username')
    if not username:
        raise InvalidUsage("Username should be specified as query paramter", username)
    res = adminControl.delete_user(username)
    if res:
        raise InvalidUsage(res, status_code=400)

    return jsonify(res)
    

# This endpoint returns the same as get_permission but add some human readable fields.
@app.route("/menu/acl", methods=['GET'])
@requires_auth
def show_human_readable_permission():
    data = g.acl
    cur2 = conn.cursor()
    result = jsonify(data.human_readable_serialize(cur2))
    # Store user stat in database
    stats_active_users.register_active_user(conn, result)
    return result

@app.route("/aggregated_stats/available_vehicles")
@requires_auth
def get_aggregated_available_vehicles_stats():
    d_filter = data_filter.DataFilter.build(request.args)
    authorized, error = g.acl.is_authorized(d_filter)
    if not authorized:
        return not_authorized(error)

    if not request.args.get("aggregation_level"):
        raise InvalidUsage("No aggregation_level specified", status_code=400)
    aggregation_level = request.args.get("aggregation_level")
    if aggregation_level not in ("day", "week", "month"):
        raise InvalidUsage("Invalid aggregation level, value should be 'day', 'week' or 'month'", status_code=400)

    if not d_filter.get_start_time():
        raise InvalidUsage("No start_time specified", status_code=400)
    if not d_filter.get_end_time():
        raise InvalidUsage("No end_time specified", status_code=400)

    result = {}
    result["available_vehicles_aggregated_stats"] = statsAggregatedAvailability.get_stats(d_filter, aggregation_level)
    return jsonify(result)

@app.route("/aggregated_stats/rentals")
@requires_auth
def get_aggregated_rental_stats():
    d_filter = data_filter.DataFilter.build(request.args)
    authorized, error = g.acl.is_authorized(d_filter)
    if not authorized:
        return not_authorized(error)

    if not request.args.get("aggregation_level"):
        raise InvalidUsage("No aggregation_level specified", status_code=400)
    aggregation_level = request.args.get("aggregation_level")
    if aggregation_level not in ("day", "week", "month"):
        raise InvalidUsage("Invalid aggregation level, value should be 'day', 'week' or 'month'", status_code=400)

    if not d_filter.get_start_time():
        raise InvalidUsage("No start_time specified", status_code=400)
    if not d_filter.get_end_time():
        raise InvalidUsage("No end_time specified", status_code=400)

    result = {}
    result["rentals_aggregated_stats"] = statsAggregatedRentals.get_stats(d_filter, aggregation_level)
    conn.commit()
    return jsonify(result)
