from typing import Optional

from fastapi import Depends, Query
from typing_extensions import Annotated

import data_filter

TIMESTAMP_PATTERN = r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}Z$"


def get_data_filter(
    zone_ids: Annotated[Optional[str], Query(description="Comma-separated list of zone ids")] = None,
    operators: Annotated[Optional[str], Query(description="Comma-separated list of operator system ids")] = None,
    municipalities: Annotated[Optional[str], Query(description="Comma-separated list of municipality gm_codes")] = None,
    form_factors: Annotated[Optional[str], Query(description="Comma-separated list of form factors")] = None,
    start_time: Annotated[Optional[str], Query(pattern=TIMESTAMP_PATTERN)] = None,
    end_time: Annotated[Optional[str], Query(pattern=TIMESTAMP_PATTERN)] = None,
    timestamp: Annotated[Optional[str], Query(pattern=TIMESTAMP_PATTERN)] = None,
    trip_source: Annotated[Optional[str], Query(pattern="^(vehicles|trips)$", description="Trip source: 'vehicles' (default) or 'trips'")] = None,
    gm_code: Annotated[Optional[str], Query(alias="gm_code", description="Deprecated, use municipalities")] = None,
) -> data_filter.DataFilter:
    args = {}
    if zone_ids:
        args["zone_ids"] = zone_ids
    if operators:
        args["operators"] = operators
    if municipalities:
        args["municipalities"] = municipalities
    if form_factors:
        args["form_factors"] = form_factors
    if start_time:
        args["start_time"] = start_time
    if end_time:
        args["end_time"] = end_time
    if timestamp:
        args["timestamp"] = timestamp
    if trip_source:
        args["trip_source"] = trip_source
    if gm_code:
        args["gm_code"] = gm_code
    return data_filter.DataFilter.build(args)


DataFilterDep = Annotated[data_filter.DataFilter, Depends(get_data_filter)]
