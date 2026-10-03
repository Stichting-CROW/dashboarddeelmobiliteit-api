from fastapi import APIRouter, HTTPException

import trips
import trips_v2
from core.filter_params import DataFilterDep
from core.security import AclUser, DbConn, not_authorized

router = APIRouter()

tripAdapter = trips.Trips()
tripAdapterV2 = trips_v2.Trips()


@router.get("/trips")
def get_trips(conn: DbConn, acl: AclUser, d_filter: DataFilterDep):
    authorized, error = acl.is_authorized(d_filter)
    if not authorized:
        return not_authorized(error)

    result = {}
    result["trips"] = tripAdapter.get_trips(conn, d_filter)
    conn.commit()
    return result


@router.get("/trips/stats")
def get_trips_stats(conn: DbConn, acl: AclUser, d_filter: DataFilterDep):
    authorized, error = acl.is_authorized(d_filter)
    if not authorized:
        return not_authorized(error)

    if not d_filter.get_start_time():
        raise HTTPException(status_code=400, detail="No start_time specified")
    if not d_filter.get_end_time():
        raise HTTPException(status_code=400, detail="No end_time specified")

    result = {}
    result["trip_stats"] = tripAdapter.get_stats(conn, d_filter)
    conn.commit()
    return result


@router.get("/v2/trips/origins")
def get_trips_origins(conn: DbConn, acl: AclUser, d_filter: DataFilterDep):
    authorized, error = acl.is_authorized(d_filter)
    if not authorized:
        return not_authorized(error)

    result = {}
    result["trip_origins"] = tripAdapterV2.get_trip_origins(conn, d_filter)
    conn.commit()
    return result


@router.get("/v2/trips/destinations")
def get_trips_destinations(conn: DbConn, acl: AclUser, d_filter: DataFilterDep):
    authorized, error = acl.is_authorized(d_filter)
    if not authorized:
        return not_authorized(error)

    result = {}
    result["trip_destinations"] = tripAdapterV2.get_trip_destinations(conn, d_filter)
    conn.commit()
    return result
