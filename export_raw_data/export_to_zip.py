import psycopg2
import psycopg2.extras
import psycopg2.sql
import os
import datetime
from report import report_stat_collector, generate_stat_xlsx
import random
import string
import os
import zipfile

def generate_zip(conn, d_filter):
    letters = string.ascii_letters
    dir_name = "/tmp/export_" + ''.join(random.choice(letters) for i in range(10))
    os.makedirs(dir_name)

    trip_file_name = generate_trips(conn, d_filter, dir_name)
    park_events_file_name = generate_park_events(conn, d_filter, dir_name)
    export_file_name = dir_name + "/export.zip"
    with zipfile.ZipFile(export_file_name, 'w', compression=zipfile.ZIP_DEFLATED) as zip_file:
        zip_file.write(trip_file_name, "trips.csv")
        zip_file.write(park_events_file_name, "park_events.csv")
    return dir_name

def generate_trips(conn, d_filter, dir_name):
    cur = conn.cursor()
    file_name = dir_name + "/trips.csv"
    f = open(file_name, "w")
    stmt = psycopg2.sql.SQL("""
    COPY 
        (
        WITH temp_a (filter_area) AS
            (
            SELECT st_union(area) 
	            FROM zones WHERE zone_id IN {zone_ids}
        )    
            
        SELECT system_id, bike_id, st_y(start_location) as lat_start_location, 
        st_x(start_location) as lng_start_location, st_y(end_location) as lat_end_location, 
        st_x(end_location) as lng_end_location, start_time, end_time, 
        st_distancesphere(start_location, end_location) as distance, EXTRACT(EPOCH FROM (end_time - start_time)) as duration_in_seconds
        FROM trips, temp_a
        WHERE start_time >= {start_time}
        AND start_time < {end_time}
        AND (
                false = {filter_on_zones} or (
                    ST_Within(start_location, temp_a.filter_area) OR
                    ST_Within(end_location, temp_a.filter_area)
                )
            )
            AND (
                false = {filter_on_system_id} or 
                system_id IN {system_ids}
            )
        )
        TO STDOUT With CSV HEADER DELIMITER ','
    """).format(
        start_time=psycopg2.sql.Literal(d_filter.start_time),
        end_time=psycopg2.sql.Literal(d_filter.end_time),
        filter_on_zones=psycopg2.sql.Literal(d_filter.has_zone_filter()),
        zone_ids=psycopg2.sql.Literal(d_filter.get_zones()),
        filter_on_system_id=psycopg2.sql.Literal(d_filter.has_operator_filter()),
        system_ids=psycopg2.sql.Literal(d_filter.get_operators())
    )
    cur.copy_expert(stmt, f)
    f.close()

    return file_name

def generate_park_events(conn, d_filter, dir_name):
    cur = conn.cursor()
    file_name = dir_name + "/park_events.csv"
    f = open(file_name, "w")
    stmt = psycopg2.sql.SQL("""COPY 
    (SELECT system_id, bike_id,
    ST_Y(location) as lat, ST_X(location) as lon,
    start_time, end_time, park_event_id, 
    check_out_sample_id, check_in_sample_id  
    FROM park_events 
    WHERE (start_time >= {start_time} and start_time < {end_time})
    AND (
            false = {filter_on_zones} 
            or ST_WITHIN(location, (
                SELECT st_union(area) 
	            FROM zones WHERE zone_id IN {zone_ids}
            ))
        )
        AND (false = {filter_on_system_id} or system_id IN {system_ids})
    ) To STDOUT With CSV HEADER DELIMITER ','
    """).format(
        start_time=psycopg2.sql.Literal(d_filter.start_time),
        end_time=psycopg2.sql.Literal(d_filter.end_time),
        filter_on_zones=psycopg2.sql.Literal(d_filter.has_zone_filter()),
        zone_ids=psycopg2.sql.Literal(d_filter.get_zones()),
        filter_on_system_id=psycopg2.sql.Literal(d_filter.has_operator_filter()),
        system_ids=psycopg2.sql.Literal(d_filter.get_operators())
    )
    cur.copy_expert(stmt, f)
    f.close()

    return file_name

