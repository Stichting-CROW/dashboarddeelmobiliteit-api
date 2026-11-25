from datetime import date, datetime, timedelta, timezone

import pytest

from core.database import get_conn
from core.security import get_acl_user
from data_quality import feed_status
from data_quality.feed_status import TimeInterval

T0 = datetime(2026, 11, 1, tzinfo=timezone.utc)
HOUR = timedelta(hours=1)
DAY = timedelta(days=1)


def interval(start_h, end_h=None):
    return TimeInterval(
        start_time=T0 + start_h * HOUR,
        end_time=None if end_h is None else T0 + end_h * HOUR,
    )


def test_feed_name_drops_credentials_and_query_string():
    assert feed_status.feed_name(
        "https://user:secret@gbfs.example.com:8443/rotterdam/free_bike_status.json?key=abc"
    ) == "gbfs.example.com:8443/rotterdam/free_bike_status.json"


def test_merge_intervals_joins_overlapping_and_open_intervals():
    merged = feed_status.merge_intervals([
        interval(5, 6), interval(0, 2), interval(1, 3), interval(8), interval(9, 10),
    ])
    assert [(m.start_time, m.end_time) for m in merged] == [
        (T0, T0 + 3 * HOUR), (T0 + 5 * HOUR, T0 + 6 * HOUR), (T0 + 8 * HOUR, None),
    ]


def test_availability_counts_only_the_elapsed_part_of_the_period():
    now = T0 + 10 * HOUR
    assert feed_status.availability_pct([interval(-5, 1)], T0, T0 + DAY, now) == 90.0
    assert feed_status.availability_pct([interval(9)], T0, T0 + DAY, now) == 90.0
    assert feed_status.availability_pct([], T0 + DAY, T0 + 2 * DAY, now) is None


@pytest.mark.parametrize("statuses,expected", [
    ([], "not_expected"),
    (["available", "unavailable"], "unavailable"),
    (["unknown", "unknown"], "unknown"),
    (["unknown", "available"], "available"),
])
def test_combined_status(statuses, expected):
    assert feed_status._combined_status(statuses) == expected


class FakeCursor:
    def __init__(self, recorded):
        self.recorded = recorded
        self.rows = []

    def execute(self, stmt, params):
        self.recorded.append(params)
        if "NOW()" in stmt:
            self.rows = [(T0, T0 + 7 * DAY, T0 + 7 * DAY)]
        elif "FROM feeds" in stmt:
            self.rows = [
                (1, "lime", "vehicles", "https://a.example.com/v1/vehicles?token=x", T0, None),
                (2, "lime", "vehicles", "https://b.example.com/v1/vehicles", T0, None),
                (3, "lime", "trips", "https://a.example.com/v1/trips", None, None),
                (4, "dott", "vehicles", "https://dott.example.com/gbfs", T0, None),
            ]
            if params["operators"] is not None:
                self.rows = [row for row in self.rows if row[1] in params["operators"]]
        elif "FROM feed_downtime" in stmt:
            self.rows = [(2, T0 + 24 * HOUR, T0 + 30 * HOUR), (2, T0 + 160 * HOUR, None)]
        elif "FROM day_statistics" in stmt:
            self.rows = [("dott",), ("cykl",)]
        else:
            self.rows = []

    def fetchall(self):
        return self.rows


class FakeConn:
    def __init__(self):
        self.recorded = []

    def cursor(self):
        return FakeCursor(self.recorded)

    def commit(self):
        pass


class FakeACL:
    username = "user@example.com"

    def __init__(self, organisation_type="ADMIN", operators=()):
        self.organisation_type = organisation_type
        self.operator_filters = set(operators)
        self.municipality_filters = set()

    def check_municipality_code(self, municipality_code):
        return True, None


def by_key(items):
    return {(item.system_id, item.data_type): item for item in items}


def test_combines_feeds_per_operator_and_data_type():
    result = by_key(feed_status.get_feed_availability(
        FakeConn(), date(2026, 11, 1), date(2026, 11, 7), None, None
    ))
    assert sorted({system_id for system_id, _ in result}) == ["dott", "lime"]

    aanbod = result[("lime", "aanbod")]
    assert aanbod.status == "unavailable"
    assert [f.status for f in aanbod.individual_feeds] == ["available", "unavailable"]
    assert aanbod.individual_feeds[0].name == "a.example.com/v1/vehicles"
    assert len(aanbod.outages) == 2
    assert aanbod.availability_pct == pytest.approx(100 * (1 - 14 / 168), abs=0.1)

    assert result[("lime", "verhuringen")].status == "unknown"
    assert result[("lime", "verhuringen")].availability_pct is None
    assert result[("lime", "servicegebieden")].status == "not_expected"
    assert result[("dott", "aanbod")].status == "available"
    assert result[("dott", "aanbod")].availability_pct == 100.0


def test_with_gm_code_only_returns_operators_active_there():
    result = by_key(feed_status.get_feed_availability(
        FakeConn(), date(2026, 11, 1), date(2026, 11, 7), "GM0599", None
    ))
    assert sorted({system_id for system_id, _ in result}) == ["cykl", "dott"]
    assert all(item.status == "not_expected" for (sid, _), item in result.items() if sid == "cykl")


@pytest.fixture
def client_as(client):
    def _client_as(acl, conn):
        client.app.dependency_overrides[get_acl_user] = lambda: acl
        client.app.dependency_overrides[get_conn] = lambda: conn
        return client

    yield _client_as
    client.app.dependency_overrides.pop(get_acl_user, None)


def test_operator_accounts_only_get_their_own_operator(client_as):
    conn = FakeConn()
    client = client_as(FakeACL("OPERATOR", operators={"dott"}), conn)
    response = client.get("/data_quality/feed_status?start_date=2026-11-01&end_date=2026-11-07")
    assert response.status_code == 200
    feeds_params = next(p for p in conn.recorded if "operators" in p)
    assert feeds_params["operators"] == ["dott"]
    assert {item["system_id"] for item in response.json()} == {"dott"}
