import json
from typing import Optional

from fastapi import APIRouter, Query
from typing_extensions import Annotated

import access_control
import park_events
import zones
from core.filter_params import DataFilterDep
from core.security import DbConn

router = APIRouter()

zoneAdapter = zones.Zones()
parkEventsAdapter = park_events.ParkEvents()
defaultAccessControl = access_control.DefaultACL()


def get_municipality_area(conn, municipality):
    cur = conn.cursor()
    stmt = """
        SELECT ST_AsGeoJSON(geom)
        FROM municipalities
        WHERE gm_code = %s and geom is not null"""
    cur.execute(stmt, (municipality,))
    return cur.fetchone()


@router.get("/area")
def get_areas(conn: DbConn, gm_code: Annotated[Optional[str], Query()] = None):
    output = {}
    if gm_code:
        area = get_municipality_area(conn, gm_code)[0]
        if area:
            output["geojson"] = json.loads(area)
            output["gm_code"] = gm_code

    conn.commit()
    return output


@router.get("/public/filters")
def get_filters(conn: DbConn, d_filter: DataFilterDep):
    result = {}
    result["filter_values"] = defaultAccessControl.serialize(conn)
    if d_filter.has_gmcode():
        result["filter_values"]["zones"] = zoneAdapter.list_zones(conn, d_filter, include_custom_zones=False)
    return result


@router.get("/public/vehicles_in_public_space")
def get_vehicles_in_public_space(conn: DbConn, d_filter: DataFilterDep):
    result = {}
    result["vehicles_in_public_space"] = parkEventsAdapter.get_public_park_events(conn, d_filter)
    return result


@router.get("/public/active_feeds")
def get_active_feeds(conn: DbConn):
    cur = conn.cursor()
    stmt = """
     SELECT JSON_AGG(active_feeds.*) FROM (
		SELECT feeds.feed_id, feeds.system_id, feeds.feed_type, feeds.last_time_succesfully_imported, last_time_succesfully_imported IS NOT NULL AND last_time_succesfully_imported > NOW() - INTERVAL '5 minutes' AS up
		FROM feeds
		WHERE is_active = true AND import_vehicles = true
		ORDER BY feed_id
    ) AS active_feeds;
    """
    cur.execute(stmt)

    res = cur.fetchone()[0]
    return res
