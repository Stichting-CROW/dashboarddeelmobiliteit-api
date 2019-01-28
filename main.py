from flask import Flask, jsonify, request
from flask.json import JSONEncoder
from datetime import date
import psycopg2
import os

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

app = Flask(__name__)
app.json_encoder = CustomJSONEncoder

conn_str = "dbname=deelfietsdashboard"

if "ip" in os.environ:
    conn_str += " host={} ".format(os.environ['ip'])
if "password" in os.environ:
    conn_str += " user=deelfietsdashboard password={}".format(os.environ['password'])


conn = psycopg2.connect(conn_str)
cur = conn.cursor()



@app.route("/cycles")
def bike_locations(): 
    if request.args.get('gm_code'):
        result = get_bicycles_in_municipality(request.args.get('gm_code'))
    else:
        result = get_all_bicycles()

    output = {}
    output["bicycles"] = []
    for record in result:
        output["bicycles"].append(serialize_location(record))
    return jsonify(output)

def serialize_location(result):
    data = {}
    data["last_time_position_reported"] = result[0]
    data["bike_id"] = result[1]
    data["location"] = {}
    data["location"]["latitude"] = result[2] 
    data["location"]["longitude"] = result[3]
    data["system_id"] = result[4]
    return data

def get_bicycles_in_municipality(municipality):
    stmt = """SELECT distinct ON (bike_id) last_time_imported, bike_id,
ST_Y(location), ST_X(location), system_id
FROM bike_detection
WHERE ST_WITHIN(location, 
	(SELECT geom from municipalities where gm_code='GM0479' and geom is not null limit 1) )
ORDER BY bike_id, last_time_imported DESC"""
    cur.execute(stmt)
    return cur.fetchall()

def get_all_bicycles():
    stmt = """SELECT distinct ON (bike_id) last_time_imported, bike_id,
            ST_Y(location), ST_X(location), system_id
            FROM bike_detection
            ORDER BY bike_id, last_time_imported DESC"""
    cur.execute(stmt)
    return cur.fetchall()


