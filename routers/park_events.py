from typing import Any

from fastapi import APIRouter, Body
from typing_extensions import Annotated

import data_filter
import park_events
from core.filter_params import DataFilterDep
from core.security import AclUser, DbConn, not_authorized

router = APIRouter()

parkEventsAdapter = park_events.ParkEvents()


@router.get("/park_events")
def get_park_events(conn: DbConn, acl: AclUser, d_filter: DataFilterDep):
    authorized, error = acl.is_authorized(d_filter)
    if not authorized:
        return not_authorized(error)

    result = {}
    result["park_events"] = parkEventsAdapter.get_private_park_events(conn, d_filter)
    return result


@router.get("/v2/park_events/stats")
def get_park_events_stats_v2(conn: DbConn, acl: AclUser, d_filter: DataFilterDep):
    authorized, error = acl.is_authorized(d_filter)
    if not authorized:
        return not_authorized(error)

    result = {}
    result["park_event_stats"] = parkEventsAdapter.get_park_event_stats(conn, d_filter)
    return result


@router.get("/public/park_events/stats")
def get_public_park_events_stats(conn: DbConn, d_filter: DataFilterDep):
    result = {}
    result["park_event_stats"] = parkEventsAdapter.get_public_park_event_stats(conn, d_filter)
    return result


@router.post("/parkeertelling")
def get_parkeertelling(conn: DbConn, request_data: Annotated[dict[str, Any], Body()]):
    d_filter = data_filter.DataFilter.build(request_data)
    result = parkEventsAdapter.parkeertelling(conn, d_filter)
    conn.commit()
    return result
