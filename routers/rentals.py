from fastapi import APIRouter, HTTPException

import rentals
from core.filter_params import DataFilterDep
from core.security import AclUser, DbConn, not_authorized

router = APIRouter()

rentalAdapter = rentals.Rentals()


@router.get("/rentals")
def get_rentals(conn: DbConn, acl: AclUser, d_filter: DataFilterDep):
    authorized, error = acl.is_authorized(d_filter)
    if not authorized:
        return not_authorized(error)

    result = {}
    result["start_rentals"] = rentalAdapter.get_start_trips(conn, d_filter)
    result["end_rentals"] = rentalAdapter.get_end_trips(conn, d_filter)
    conn.commit()
    return result


@router.get("/rentals/stats")
def get_rentals_stats(conn: DbConn, acl: AclUser, d_filter: DataFilterDep):
    authorized, error = acl.is_authorized(d_filter)
    if not authorized:
        return not_authorized(error)

    if not d_filter.get_start_time():
        raise HTTPException(status_code=400, detail="No start_time specified")
    if not d_filter.get_end_time():
        raise HTTPException(status_code=400, detail="No end_time specified")

    result = {}
    result["rental_stats"] = rentalAdapter.get_stats(conn, d_filter)
    conn.commit()
    return result
