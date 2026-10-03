from typing import Literal

from fastapi import APIRouter, HTTPException

import stats_aggregated_availability
import stats_aggregated_rentals
import stats_v2.availability_stats as availability_stats
import stats_v2.rental_stats as rental_stats
from core.database import get_timescaledb_conn
from core.filter_params import DataFilterDep
from core.security import AclUser, DbConn, TimeScaleConn, not_authorized

router = APIRouter()

statsAggregatedAvailability = stats_aggregated_availability.AggregatedStatsAvailability()
statsAggregatedRentals = stats_aggregated_rentals.AggregatedStatsRentals()
availabilityStatsAdapter = availability_stats.AvailabilityStats()
rentalStatsAdapter = rental_stats.RentalStats()

AggregationLevelDayWeekMonth = Literal["day", "week", "month"]
AggregationLevelTimeBucket = Literal["5m", "15m", "hour", "day", "week", "month"]
AggregationFunction = Literal["MIN", "MAX", "AVG"]
GroupBy = Literal["operator", "modality"]


@router.get("/aggregated_stats/available_vehicles")
def get_aggregated_available_vehicles_stats(
    conn: DbConn,
    acl: AclUser,
    d_filter: DataFilterDep,
    aggregation_level: AggregationLevelDayWeekMonth,
):
    authorized, error = acl.is_authorized(d_filter)
    if not authorized:
        return not_authorized(error)

    if not d_filter.get_start_time():
        raise HTTPException(status_code=400, detail="No start_time specified")
    if not d_filter.get_end_time():
        raise HTTPException(status_code=400, detail="No end_time specified")

    result = {}
    result["available_vehicles_aggregated_stats"] = statsAggregatedAvailability.get_stats(conn, d_filter, aggregation_level)
    return result


@router.get("/aggregated_stats/rentals")
def get_aggregated_rental_stats(
    conn: DbConn,
    acl: AclUser,
    d_filter: DataFilterDep,
    aggregation_level: AggregationLevelDayWeekMonth,
):
    authorized, error = acl.is_authorized(d_filter)
    if not authorized:
        return not_authorized(error)

    if not d_filter.get_start_time():
        raise HTTPException(status_code=400, detail="No start_time specified")
    if not d_filter.get_end_time():
        raise HTTPException(status_code=400, detail="No end_time specified")

    result = {}
    result["rentals_aggregated_stats"] = statsAggregatedRentals.get_stats(conn, d_filter, aggregation_level)
    return result


@router.get("/stats_v2/availability_stats")
def get_availability_stats(
    timescaledb_conn: TimeScaleConn,
    acl: AclUser,
    d_filter: DataFilterDep,
    aggregation_level: AggregationLevelTimeBucket,
    aggregation_function: AggregationFunction,
    group_by: GroupBy,
):
    authorized, error = acl.is_authorized(d_filter)
    if not authorized:
        return not_authorized(error)

    if not d_filter.get_start_time():
        raise HTTPException(status_code=400, detail="No start_time specified")
    if not d_filter.get_end_time():
        raise HTTPException(status_code=400, detail="No end_time specified")

    result = {}
    result["availability_stats"] = availabilityStatsAdapter.get_availability_stats(
        timescaledb_conn, d_filter, aggregation_level, group_by, aggregation_function
    )
    timescaledb_conn.commit()
    return result


@router.get("/stats_v2/rental_stats")
def get_rental_stats(
    timescaledb_conn: TimeScaleConn,
    acl: AclUser,
    d_filter: DataFilterDep,
    aggregation_level: AggregationLevelTimeBucket,
):
    authorized, error = acl.is_authorized(d_filter)
    if not authorized:
        return not_authorized(error)

    if not d_filter.get_start_time():
        raise HTTPException(status_code=400, detail="No start_time specified")
    if not d_filter.get_end_time():
        raise HTTPException(status_code=400, detail="No end_time specified")

    result = {}
    result["rental_stats"] = rentalStatsAdapter.get_rental_stats(timescaledb_conn, d_filter, aggregation_level)
    timescaledb_conn.commit()
    return result
