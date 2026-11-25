from datetime import date
from typing import Optional

from fastapi import APIRouter, HTTPException, Query
from typing_extensions import Annotated

from core.security import AclUser, DbConn, not_authorized
from stats_v3.config import Aggregation, Statistic, TIME_SERIES_STATISTICS, TripSource
from stats_v3.service import StatisticsService

router = APIRouter()


@router.get("/stats_v3/statistics")
def get_statistics(
    conn: DbConn,
    acl: AclUser,
    statistic: Annotated[
        Statistic,
        Query(description="The statistic to return, one graph per request"),
    ],
    municipality: Annotated[
        str,
        Query(min_length=1, description="Municipality GM code, e.g. GM0599"),
    ],
    start_date: Annotated[date, Query(description="Start date (inclusive)")],
    end_date: Annotated[date, Query(description="End date (inclusive)")],
    aggregation: Annotated[
        Optional[Aggregation],
        Query(description="Required for time-series statistics"),
    ] = None,
    trip_source: Annotated[
        Optional[TripSource],
        Query(description="Statistics source, defaults to vehicles"),
    ] = None,
):
    authorized, error = acl.check_municipality_code(municipality)
    if not authorized:
        return not_authorized(error)

    if acl.organisation_type != "ADMIN" and municipality not in acl.municipality_filters:
        return not_authorized("User is not allowed to access this municipality.")

    if end_date < start_date:
        raise HTTPException(
            status_code=400,
            detail="end_date must be on or after start_date",
        )

    if statistic in TIME_SERIES_STATISTICS and aggregation is None:
        raise HTTPException(
            status_code=400,
            detail="aggregation is required for this statistic",
        )

    service = StatisticsService(conn)
    try:
        result = service.get_statistics(
            statistic,
            municipality,
            start_date,
            end_date,
            aggregation,
            trip_source,
        )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc

    conn.commit()
    return result
