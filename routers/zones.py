from typing import Any

from fastapi import APIRouter, Body, HTTPException, Query
from typing_extensions import Annotated

import data_filter
import zones
from core.filter_params import DataFilterDep
from core.security import AclUser, DbConn, not_authorized

router = APIRouter()

zoneAdapter = zones.Zones()


def _get_zones_response(conn, d_filter: data_filter.DataFilter, include_geojson: bool):
    result = {}
    if include_geojson:
        result["zones"] = zoneAdapter.get_zones(conn, d_filter)
    else:
        result["zones"] = zoneAdapter.list_zones(conn, d_filter)
    conn.commit()
    return result


@router.get("/zones")
def get_zones(conn: DbConn, d_filter: DataFilterDep, include_geojson: bool = False):
    if not (d_filter.has_zone_filter() or d_filter.has_municipalities()):
        raise HTTPException(status_code=400, detail="No gm_code (deprecated), zone_ids or municipalities.")
    return _get_zones_response(conn, d_filter, include_geojson)


@router.delete("/zone/{zone_id}")
def delete_zone(zone_id: str, conn: DbConn, acl: AclUser):
    d_filter = data_filter.DataFilter()
    d_filter.add_zone(zone_id)
    authorized, error = acl.is_authorized(d_filter)
    if not authorized:
        return not_authorized(error)

    deleted = zoneAdapter.delete_zone(conn, zone_id)
    conn.commit()
    return {"deleted": deleted}


@router.put("/zone", status_code=201)
@router.post("/zone", status_code=201)
def insert_zone(conn: DbConn, acl: AclUser, zone_data: Annotated[dict[str, Any], Body()]):
    if "municipality" not in zone_data:
        return not_authorized("No field 'municipality' in JSON")

    authorized, error = acl.check_municipality_code(zone_data["municipality"])
    if not authorized:
        return not_authorized(error)

    result, err = zoneAdapter.create_zone(conn, zone_data)
    if err:
        raise HTTPException(status_code=400, detail=err)
    return result


@router.get("/public/zones")
def get_public_zones(conn: DbConn, d_filter: DataFilterDep, include_geojson: bool = False):
    if not (d_filter.has_municipalities() or d_filter.has_zone_filter()):
        raise HTTPException(status_code=400, detail="No gm_code (deprecated) or zone_ids, municipalities.")
    return _get_zones_response(conn, d_filter, include_geojson)


@router.get("/public/municipalities")
def get_municipalities(conn: DbConn):
    result = {}
    result["municipalities"] = zoneAdapter.list_municipalities(conn)
    conn.commit()
    return result


@router.get("/public/get_municipality_based_on_latlng")
def get_municipality_based_on_latlng(conn: DbConn, location: Annotated[str, Query()]):
    location_split = location.split(",")
    if len(location_split) != 2:
        raise HTTPException(status_code=400, detail="Location not correctly formatted, '52.0,5.0' is expected")
    lat = location_split[0]
    lng = location_split[1]

    res = zoneAdapter.get_municipality_based_on_latlng(conn, lat, lng)
    if res is None:
        raise HTTPException(status_code=404, detail="No municipality found for these coordinates")
    return res
