from decimal import Decimal
from flask import Flask, jsonify, request, g, abort, send_file, after_this_request, send_from_directory
from functools import wraps

from flask.json import JSONEncoder
from psycopg2.pool import SimpleConnectionPool
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
import stats_over_time
import rentals
import report.generate_xlsx
import export_raw_data.create_export_task
import public_zoning_stats
import audit_log
import stats_active_users
import stats_aggregated_availability
import stats_aggregated_rentals
import stats_v2.availability_stats as availability_stats
import stats_v2.rental_stats as rental_stats
from redis_helper import redis_helper

# Initialisation
conn_str = "dbname=deelfietsdashboard"

if "DB_HOST" in os.environ:
    conn_str += " host={} ".format(os.environ['DB_HOST'])
if "DB_USER" in os.environ:
    conn_str += " user={}".format(os.environ['DB_USER'])
if "DB_PASSWORD" in os.environ:
    conn_str += " password={}".format(os.environ['DB_PASSWORD'])
if "DB_PORT" in os.environ:
    conn_str += " port={}".format(os.environ['DB_PORT'])

# conn = psycopg2.connect(conn_str)
print(conn_str)
pgpool = SimpleConnectionPool(minconn=1, 
        maxconn=10, 
        dsn=conn_str)

conn_str_timescale_db = "dbname=dashboardeelmobiliteit-timescaledb"
if os.getenv('DEV') == 'true':
    conn_str_timescale_db = "dbname=dashboardeelmobiliteit-timescaledb-dev"

if "TIMESCALE_DB_HOST" in os.environ:
    conn_str_timescale_db += " host={} ".format(os.environ['TIMESCALE_DB_HOST'])
if "TIMESCALE_DB_USER" in os.environ:
    conn_str_timescale_db += " user={}".format(os.environ['TIMESCALE_DB_USER'])
if "TIMESCALE_DB_PASSWORD" in os.environ:
    conn_str_timescale_db += " password={}".format(os.environ['TIMESCALE_DB_PASSWORD'])
if "TIMESCALE_DB_PORT" in os.environ:
    conn_str_timescale_db += " port={}".format(os.environ['TIMESCALE_DB_PORT'])

timescaledb_pgpool = SimpleConnectionPool(minconn=1, 
        maxconn=10, 
        dsn=conn_str_timescale_db)

tripAdapter = trips.Trips()
tripAdapterV2 = trips_v2.Trips()
zoneAdapter = zones.Zones()
rentalAdapter = rentals.Rentals()
defaultAccessControl = access_control.DefaultACL()
accessControl = access_control.AccessControl()
statsOvertime = stats_over_time.StatsOverTime()
statsAggregatedAvailability = stats_aggregated_availability.AggregatedStatsAvailability()
statsAggregatedRentals = stats_aggregated_rentals.AggregatedStatsRentals()
parkEventsAdapter = park_events.ParkEvents()
availabilityStatsAdapter = availability_stats.AvailabilityStats()
rentalStatsAdapter = rental_stats.RentalStats()

# Custom JSON serializer to output timestamps as ISO8601
class CustomJSONEncoder(JSONEncoder):
    def default(self, obj):
        try:
            if isinstance(obj, date):
                return obj.isoformat() + "Z"
            if isinstance(obj, Decimal):
                return float(obj)
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
        g.acl = accessControl.retrieve_acl_user(request, get_conn())
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

def get_conn():
    if 'db' not in g:
        g.db = pgpool.getconn()
    return g.db

def get_timescaledb_conn():
    if 'timescaledb' not in g:
        g.timescaledb = timescaledb_pgpool.getconn()
    return g.timescaledb

@app.teardown_appcontext
def close_db(e=None):
    db = g.pop('db', None)

    if db is not None:
        pgpool.putconn(db)

    timescaledb = g.pop('timescaledb', None)

    if timescaledb is not None:
        timescaledb_pgpool.putconn(timescaledb)
    

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

def get_bicycles_within_bounding_box(sw_lng, sw_lat, ne_lng, ne_lat):
    conn = get_conn()
    cur = conn.cursor()
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
    conn = get_conn()
    cur = conn.cursor()
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
    conn = get_conn()
    cur = conn.cursor()
    stmt = """SELECT last_time_imported, last_detection_cycle.bike_id,
            ST_Y(location), ST_X(location), last_detection_cycle.system_id, 
            is_check_in, is_check_out
            FROM last_detection_cycle"""
    cur.execute(stmt)
    return cur.fetchall()

@app.route("/area")
def get_areas():
    conn = get_conn()
    output = {}
    if request.args.get('gm_code'):
        area = get_municipality_area(conn, request.args.get('gm_code'))[0]
        if area:
            output["geojson"] = json.loads(area)
            output["gm_code"] = request.args.get('gm_code')

    conn.commit()
    return jsonify(output)

def get_municipality_area(conn, municipality):
    cur = conn.cursor()
    stmt = """
        SELECT ST_AsGeoJSON(geom)
        FROM municipalities
        WHERE gm_code = %s and geom is not null"""
    cur.execute(stmt, (municipality,))
    return cur.fetchone()

@app.route("/trips")
@requires_auth
def get_trips():
    conn = get_conn()
    d_filter = data_filter.DataFilter.build(request.args)
    authorized, error = g.acl.is_authorized(d_filter)
    if not authorized:
        return not_authorized(error)

    result = {}
    result["trips"] = tripAdapter.get_trips(conn, d_filter)
    conn.commit()
    return jsonify(result)

@app.route("/trips/stats")
@requires_auth
def get_trips_stats():
    conn = get_conn()
    d_filter = data_filter.DataFilter.build(request.args)
    authorized, error = g.acl.is_authorized(d_filter)
    if not authorized:
        return not_authorized(error)

    if not d_filter.get_start_time():
        raise InvalidUsage("No start_time specified", status_code=400)
    if not d_filter.get_end_time():
        raise InvalidUsage("No end_time specified", status_code=400)

    result = {}
    result["trip_stats"] = tripAdapter.get_stats(conn, d_filter)
    conn.commit()
    return jsonify(result)


@app.route("/v2/trips/origins")
@requires_auth
def get_trips_origins():
    conn = get_conn()
    d_filter = data_filter.DataFilter.build(request.args)
    authorized, error = g.acl.is_authorized(d_filter)
    if not authorized:
        return not_authorized(error)

    result = {}
    result["trip_origins"] = tripAdapterV2.get_trip_origins(conn, d_filter)
    conn.commit()
    return jsonify(result)


@app.route("/v2/trips/destinations")
@requires_auth
def get_trips_destinations():
    conn = get_conn()

    d_filter = data_filter.DataFilter.build(request.args)
    authorized, error = g.acl.is_authorized(d_filter)
    if not authorized:
        return not_authorized(error)
    
    result = {}
    result["trip_destinations"] = tripAdapterV2.get_trip_destinations(conn, d_filter)
    print(time.time())
    conn.commit()
    return jsonify(result)

@app.route("/rentals")
@requires_auth
def get_rentals():
    conn = get_conn()

    d_filter = data_filter.DataFilter.build(request.args)
    authorized, error = g.acl.is_authorized(d_filter)
    if not authorized:
        return not_authorized(error)

    result = {}
    result["start_rentals"] = rentalAdapter.get_start_trips(conn, d_filter)
    result["end_rentals"] = rentalAdapter.get_end_trips(conn, d_filter)
    conn.commit()
    return jsonify(result)

@app.route("/rentals/stats")
@requires_auth
def get_rentals_stats():
    conn = get_conn()

    d_filter = data_filter.DataFilter.build(request.args)
    authorized, error = g.acl.is_authorized(d_filter)
    if not authorized:
        return not_authorized(error)

    if not d_filter.get_start_time():
        raise InvalidUsage("No start_time specified", status_code=400)
    if not d_filter.get_end_time():
        raise InvalidUsage("No end_time specified", status_code=400)

    result = {}
    result["rental_stats"] = rentalAdapter.get_stats(conn, d_filter)
    conn.commit()
    return jsonify(result)

@app.route("/zones")
def get_zones():
    conn = get_conn()

    d_filter = data_filter.DataFilter.build(request.args)
    if not (d_filter.has_gmcode() or d_filter.has_zone_filter()):
        raise InvalidUsage("No gm_code or zone_ids.", status_code=400)
    
    result = {}
    if request.args.get("include_geojson") and request.args.get("include_geojson") == 'true':
        result["zones"] = zoneAdapter.get_zones(conn, d_filter)
    else:
        result["zones"] = zoneAdapter.list_zones(conn, d_filter) 

    conn.commit()
    return jsonify(result)

@app.route("/zone/<zone_id>", methods=['DELETE'])
@requires_auth
def zone(zone_id):
    conn = get_conn()

    d_filter = data_filter.DataFilter()
    d_filter.add_zone(zone_id)
    authorized, error = g.acl.is_authorized(d_filter)
    if not authorized:
        return not_authorized(error)

    deleted = zoneAdapter.delete_zone(conn, zone_id)
    conn.commit()
    return jsonify({"deleted": deleted})

@app.route("/zone", methods=['PUT', 'POST'])
@requires_auth
def insert_zone():
    conn = get_conn()
    try:
        zone_data = json.loads(request.data)
    except:
        raise InvalidUsage("invalid JSON", status_code=400)
    
    if not "municipality" in zone_data:
        return not_authorized("No field 'municipality' in JSON")

    authorized, error = g.acl.check_municipality_code(zone_data["municipality"])
    if not authorized:
        return not_authorized(error)


    result, err = zoneAdapter.create_zone(conn, zone_data)
    if err:
        raise InvalidUsage(err, status_code=400)
    return jsonify(result), 201

# publicZonesAdapter = public_zoning_stats.PublicZoningStats(conn)
@app.route("/public/zones", methods=['GET'])
def get_public_zones():
    conn = get_conn()
    d_filter = data_filter.DataFilter.build(request.args)
    if not (d_filter.has_gmcode() or d_filter.has_zone_filter()):
        raise InvalidUsage("No gm_code or zone_ids.", status_code=400)

    result = {}
    if request.args.get("include_geojson") and request.args.get("include_geojson") == 'true':
        result["zones"] = zoneAdapter.get_zones(conn, d_filter)
    else:
        result["zones"] = zoneAdapter.list_zones(conn, d_filter) 

    conn.commit()
    return jsonify(result)
    # result = publicZonesAdapter.get_zones()
    # return jsonify(result)

@app.route("/public/vehicles_in_public_space", methods=['GET'])
def get_vehicles_in_public_space():
    conn = get_conn()
    d_filter = data_filter.DataFilter.build(request.args)
    
    result = {}
    result["vehicles_in_public_space"] = parkEventsAdapter.get_public_park_events(conn, d_filter) 
    return jsonify(result)


@app.route("/public/filters", methods=['GET'])
def get_filters():
    conn = get_conn()
    d_filter = data_filter.DataFilter.build(request.args)
    
    result = {}
    result["filter_values"] = defaultAccessControl.serialize(conn)
    print(d_filter.has_gmcode())
    if d_filter.has_gmcode():
        result["filter_values"]["zones"] = zoneAdapter.list_zones(conn, d_filter, include_custom_zones=False)
    return jsonify(result)


@app.route("/park_events", methods=['GET'])
@requires_auth
def get_park_events():
    conn = get_conn()
    d_filter = data_filter.DataFilter.build(request.args)
    authorized, error = g.acl.is_authorized(d_filter)
    if not authorized:
        return not_authorized(error)

    result = {}
    result["park_events"] = parkEventsAdapter.get_private_park_events(conn, d_filter) 
    return jsonify(result)

@app.route("/park_events/stats")
@requires_auth
def get_park_events_stats():
    conn = get_conn()
    d_filter = data_filter.DataFilter.build(request.args)
    authorized, error = g.acl.is_authorized(d_filter)
    if not authorized:
        return not_authorized(error)

    result = {}
    result["park_event_stats"] = parkEventsAdapter.get_stats(conn, d_filter) 
    return jsonify(result)

@app.route("/v2/park_events/stats")
@requires_auth
def get_park_events_stats_v2():
    conn = get_conn()
    d_filter = data_filter.DataFilter.build(request.args)
    authorized, error = g.acl.is_authorized(d_filter)
    if not authorized:
        return not_authorized(error)

    result = {}
    result["park_event_stats"] = parkEventsAdapter.get_park_event_stats(conn, d_filter) 
    return jsonify(result)


# In theory it's possible to retreive data from custom zones. That is not really a problem but can be fixed in the future.
@app.route("/public/park_events/stats")
def get_public_park_events_stats():
    conn = get_conn()
    d_filter = data_filter.DataFilter.build(request.args)

    result = {}
    result["park_event_stats"] = parkEventsAdapter.get_public_park_event_stats(conn, d_filter) 
    return jsonify(result)

@app.route("/stats/available_bikes")
@requires_auth
def get_available_bicycles():
    conn = get_conn()
    d_filter = data_filter.DataFilter.build(request.args)
    authorized, error = g.acl.is_authorized(d_filter)
    if not authorized:
        return not_authorized(error)

    if not d_filter.get_start_time():
        raise InvalidUsage("No start_time specified", status_code=400)
    if not d_filter.get_end_time():
        raise InvalidUsage("No end_time specified", status_code=400)
    
    result = {}
    result["available_bikes"] = statsOvertime.query_stats(conn, d_filter)
    return jsonify(result)

@app.route("/stats/generate_report")
@requires_auth
def get_report():
    conn = get_conn()
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
    conn = get_conn()
    d_filter = data_filter.DataFilter.build(request.args)
    if not d_filter.get_start_time():
        raise InvalidUsage("No start_time specified", status_code=400)
    if not d_filter.get_end_time():
        raise InvalidUsage("No end_time specified", status_code=400)
    
    # load filters
    result = d_filter.add_filters_based_on_acl(g.acl)
    if result != None:
        return not_authorized(result) 
    
    authorized_acl, error = g.acl.is_authorized(d_filter)
    if not authorized_acl:
        return not_authorized(error)

    # check if all authorizations are matched.
    authorized = g.acl.is_authorized_for_raw_data()
    if not authorized:
        return not_authorized("This user is not admin and doesn't have raw data rights.")

    audit_log.log_request(conn, g.acl.username, request.full_path, d_filter)
    with redis_helper.get_resource() as r:
        result = export_raw_data.create_export_task.schedule_export(r, d_filter, g.acl.username)
        return jsonify(result)
    

# This endpoint returns the same as get_permission but add some human readable fields.
@app.route("/menu/acl", methods=['GET'])
@requires_auth
def show_human_readable_permission():
    data = g.acl
    conn = get_conn()
    cur2 = conn.cursor()
    result = data.human_readable_serialize(cur2)
    # Store user stat in database
    stats_active_users.register_active_user(conn, result)
    return jsonify(result)

@app.route("/aggregated_stats/available_vehicles")
@requires_auth
def get_aggregated_available_vehicles_stats():
    conn = get_conn()
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
    result["available_vehicles_aggregated_stats"] = statsAggregatedAvailability.get_stats(conn, d_filter, aggregation_level)
    return jsonify(result)

@app.route("/stats_v2/availability_stats")
@requires_auth
def get_availability_stats():
    timescaledb_conn = get_timescaledb_conn()
    d_filter = data_filter.DataFilter.build(request.args)
    authorized, error = g.acl.is_authorized(d_filter)
    if not authorized:
        return not_authorized(error)

    if not request.args.get("aggregation_level"):
        raise InvalidUsage("No aggregation_level specified", status_code=400)
    aggregation_level = request.args.get("aggregation_level")
    if aggregation_level not in ("5m", "15m", "hour", "day", "week", "month"):
        raise InvalidUsage("Invalid aggregation_level, value should be '5m', '15m', 'hour', 'day', 'week' or 'month'", status_code=400)

    aggregation_function = request.args.get("aggregation_function")
    if aggregation_function not in ("MIN", "MAX", "AVG"):
        raise InvalidUsage("Invalid aggregation_function, value should be 'MIN', 'MAX' or 'AVG''", status_code=400)

    if not request.args.get("group_by"):
        raise InvalidUsage("No group_by specified", status_code=400)
    group_by = request.args.get("group_by")
    if group_by not in ("operator", "modality"):
        raise InvalidUsage("Invalid group_by, value should be 'operator' or 'modality'", status_code=400)

    if not d_filter.get_start_time():
        raise InvalidUsage("No start_time specified", status_code=400)
    if not d_filter.get_end_time():
        raise InvalidUsage("No end_time specified", status_code=400)

    result = {}
    result["availability_stats"] = availabilityStatsAdapter.get_availability_stats(timescaledb_conn, d_filter, aggregation_level, group_by, aggregation_function)
    timescaledb_conn.commit()
    return jsonify(result)

@app.route("/stats_v2/rental_stats")
@requires_auth
def get_rental_stats():
    timescaledb_conn = get_timescaledb_conn()
    d_filter = data_filter.DataFilter.build(request.args)
    authorized, error = g.acl.is_authorized(d_filter)
    if not authorized:
        return not_authorized(error)

    if not request.args.get("aggregation_level"):
        raise InvalidUsage("No aggregation_level specified", status_code=400)
    aggregation_level = request.args.get("aggregation_level")
    if aggregation_level not in ("5m", "15m", "hour", "day", "week", "month"):
        raise InvalidUsage("Invalid aggregation_level, value should be '5m', '15m', 'hour', 'day', 'week' or 'month'", status_code=400)

    if not d_filter.get_start_time():
        raise InvalidUsage("No start_time specified", status_code=400)
    if not d_filter.get_end_time():
        raise InvalidUsage("No end_time specified", status_code=400)

    result = {}
    result["rental_stats"] = rentalStatsAdapter.get_rental_stats(timescaledb_conn, d_filter, aggregation_level)
    timescaledb_conn.commit()
    return jsonify(result)

@app.route("/aggregated_stats/rentals")
@requires_auth
def get_aggregated_rental_stats():
    conn = get_conn()
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
    result["rentals_aggregated_stats"] = statsAggregatedRentals.get_stats(conn, d_filter, aggregation_level)
    conn.commit()
    return jsonify(result)

