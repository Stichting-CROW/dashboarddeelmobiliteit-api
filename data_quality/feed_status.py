from collections import defaultdict
from datetime import date, datetime, timedelta
from typing import Literal, Optional
from urllib.parse import urlsplit

from pydantic import BaseModel

from data_quality.rejections import API_DATA_TYPES, DB_DATA_TYPES, TIMEZONE

FeedStatus = Literal["available", "unavailable", "not_expected", "unknown"]


class TimeInterval(BaseModel):
    start_time: datetime
    end_time: Optional[datetime] = None


class IndividualFeed(BaseModel):
    feed_id: int
    name: str
    status: Literal["available", "unavailable", "unknown"]
    last_data_received_at: Optional[datetime] = None
    outages: list[TimeInterval]


class FeedAvailability(BaseModel):
    system_id: str
    data_type: Literal["aanbod", "verhuringen", "servicegebieden"]
    status: FeedStatus
    last_checked_at: Optional[datetime] = None
    last_data_received_at: Optional[datetime] = None
    availability_pct: Optional[float] = None
    outages: list[TimeInterval]
    individual_feeds: list[IndividualFeed]


def feed_name(feed_url: str) -> str:
    """Host and path of a feed URL, without credentials or query string."""
    parts = urlsplit(feed_url)
    host = parts.hostname or ""
    if parts.port:
        host = "{}:{}".format(host, parts.port)
    return (host + parts.path) or feed_url.split("?")[0]


def merge_intervals(intervals: list[TimeInterval]) -> list[TimeInterval]:
    merged: list[TimeInterval] = []
    for interval in sorted(intervals, key=lambda i: i.start_time):
        last = merged[-1] if merged else None
        if last and (last.end_time is None or interval.start_time <= last.end_time):
            if last.end_time is not None and (
                interval.end_time is None or interval.end_time > last.end_time
            ):
                last.end_time = interval.end_time
            continue
        merged.append(interval.model_copy())
    return merged


def availability_pct(
    outages: list[TimeInterval], period_start: datetime, period_end: datetime, now: datetime
) -> Optional[float]:
    """Share of the elapsed part of the period without a (merged) outage."""
    window_end = min(period_end, now)
    elapsed = (window_end - period_start).total_seconds()
    if elapsed <= 0:
        return None
    down = 0.0
    for outage in outages:
        start = max(outage.start_time, period_start)
        end = min(outage.end_time or window_end, window_end)
        if end > start:
            down += (end - start).total_seconds()
    return round(100 * (1 - down / elapsed), 1)


def _feed_status(feed: dict, has_open_downtime: bool) -> str:
    if has_open_downtime:
        return "unavailable"
    if feed["last_success"] is None and feed["last_attempt"] is None:
        return "unknown"
    return "available"


def _combined_status(feed_statuses: list[str]) -> str:
    if not feed_statuses:
        return "not_expected"
    if "unavailable" in feed_statuses:
        return "unavailable"
    if all(status == "unknown" for status in feed_statuses):
        return "unknown"
    return "available"


def _fetchall(conn, stmt, params):
    cur = conn.cursor()
    cur.execute(stmt, params)
    return cur.fetchall()


def _period_bounds(conn, start_date: date, end_date: date):
    rows = _fetchall(conn, """
        SELECT %(start_date)s::timestamp AT TIME ZONE %(tz)s,
               %(end_date)s::timestamp AT TIME ZONE %(tz)s,
               NOW()
    """, {"start_date": start_date, "end_date": end_date + timedelta(days=1), "tz": TIMEZONE})
    return rows[0]


def _operators_active_in_municipality(conn, gm_code: str, start_date: date, end_date: date):
    rows = _fetchall(conn, """
        SELECT DISTINCT s.system_id
        FROM day_statistics s
        WHERE s.date BETWEEN %(start_date)s AND %(end_date)s
        AND s.value > 0
        AND s.geometry_ref IN (
            SELECT COALESCE(g.geography_id::text, z.stats_ref)
            FROM zones z
            LEFT JOIN geographies g ON g.zone_id = z.zone_id
            WHERE z.zone_type = 'municipality'
            AND z.municipality = %(gm_code)s
            AND COALESCE(g.geography_id::text, z.stats_ref) IS NOT NULL
        )
    """, {"gm_code": gm_code, "start_date": start_date, "end_date": end_date})
    return {row[0] for row in rows}


def get_feed_availability(
    conn,
    start_date: date,
    end_date: date,
    gm_code: Optional[str],
    operators: Optional[list[str]],
) -> list[FeedAvailability]:
    """
    Availability per operator and data type over the operator's active feeds.
    `operators` limits the result to those operators; None means no limit.
    """
    period_start, period_end, now = _period_bounds(conn, start_date, end_date)

    feed_rows = _fetchall(conn, """
        SELECT feed_id, system_id, data_type::text, feed_url,
               last_time_succesfully_imported, last_time_import_attempted
        FROM feeds
        WHERE is_active = true
        AND (%(operators)s::text[] IS NULL OR system_id = ANY(%(operators)s))
        ORDER BY system_id, feed_id
    """, {"operators": operators})
    feeds = [{
        "feed_id": row[0], "system_id": row[1], "data_type": row[2], "feed_url": row[3],
        "last_success": row[4], "last_attempt": row[5],
    } for row in feed_rows]

    downtime_rows = _fetchall(conn, """
        SELECT d.feed_id, d.downtime_start, d.downtime_end
        FROM feed_downtime d
        JOIN feeds f USING (feed_id)
        WHERE f.is_active = true
        AND (d.downtime_end IS NULL
             OR (d.downtime_start < %(period_end)s AND d.downtime_end > %(period_start)s))
        ORDER BY d.downtime_start
    """, {"period_start": period_start, "period_end": period_end})
    outages_by_feed = defaultdict(list)
    open_downtime = set()
    for feed_id, start, end in downtime_rows:
        if end is None:
            open_downtime.add(feed_id)
        if start < period_end:
            outages_by_feed[feed_id].append(TimeInterval(start_time=start, end_time=end))

    if gm_code:
        system_ids = _operators_active_in_municipality(conn, gm_code, start_date, end_date)
        if operators is not None:
            system_ids &= set(operators)
    else:
        system_ids = {feed["system_id"] for feed in feeds}

    feeds_by_key = defaultdict(list)
    for feed in feeds:
        feeds_by_key[(feed["system_id"], feed["data_type"])].append(feed)

    result = []
    for system_id in sorted(system_ids):
        for db_data_type in DB_DATA_TYPES.values():
            individual = []
            for feed in feeds_by_key[(system_id, db_data_type)]:
                individual.append(IndividualFeed(
                    feed_id=feed["feed_id"],
                    name=feed_name(feed["feed_url"]),
                    status=_feed_status(feed, feed["feed_id"] in open_downtime),
                    last_data_received_at=feed["last_success"],
                    outages=outages_by_feed[feed["feed_id"]],
                ))
            status = _combined_status([feed.status for feed in individual])
            outages = merge_intervals([o for feed in individual for o in feed.outages])
            group = feeds_by_key[(system_id, db_data_type)]
            checked = [t for f in group for t in (f["last_attempt"], f["last_success"]) if t]
            received = [f["last_success"] for f in group if f["last_success"]]
            result.append(FeedAvailability(
                system_id=system_id,
                data_type=API_DATA_TYPES[db_data_type],
                status=status,
                last_checked_at=max(checked) if checked else None,
                last_data_received_at=min(received) if received else None,
                availability_pct=(
                    availability_pct(outages, period_start, period_end, now)
                    if status in ("available", "unavailable") else None
                ),
                outages=outages,
                individual_feeds=individual,
            ))
    return result
