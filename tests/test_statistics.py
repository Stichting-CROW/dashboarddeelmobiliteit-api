from collections import defaultdict
from datetime import date

import pytest

from core.database import get_conn
from core.security import get_acl_user
from stats_v3.config import DISTANCE_DISTRIBUTION, DISTRIBUTION_CONFIG
from stats_v3.service import StatisticsService
from stats_v3.time_utils import iter_periods, months_between


class FakeCursor:
    def __init__(self):
        self.rows = []

    def execute(self, query, params):
        if "COALESCE(g.geography_id" in query:
            self.rows = [("cbs:GM0599",)]
        elif "SUM(s.value) AS value" in query:
            self.rows = [(date(2026, 1, 1), "bicycle", 10)]
        elif "SELECT DISTINCT" in query:
            self.rows = [("bicycle", "lime")]
        else:
            self.rows = []

    def fetchall(self):
        return self.rows

    def close(self):
        pass


class UnavailableFakeCursor(FakeCursor):
    def execute(self, query, params):
        if "COALESCE(g.geography_id" in query:
            self.rows = [("cbs:GM0599",)]
        elif "FROM moment_statistics" in query:
            self.rows = [(date(2026, 1, 1), "bicycle", 100, 60)]
        elif "AS max_total" in query:
            self.rows = [(date(2026, 1, 1), "bicycle", 200)]
        elif "SELECT DISTINCT" in query:
            self.rows = [("bicycle", "lime")]
        else:
            self.rows = []


class FakeConn:
    def __init__(self, cursor_cls=FakeCursor):
        self.cursor_cls = cursor_cls

    def cursor(self):
        return self.cursor_cls()

    def commit(self):
        pass


class AllowMunicipalityACL:
    organisation_type = "MUNICIPALITY"
    municipality_filters = {"GM0599"}

    def check_municipality_code(self, municipality):
        return True, None


class DenyMunicipalityACL:
    organisation_type = "MUNICIPALITY"
    municipality_filters = {"GM0001"}

    def check_municipality_code(self, municipality):
        return False, "User is not allowed to access this municipality."


@pytest.fixture
def stats_client(client):
    client.app.dependency_overrides[get_acl_user] = lambda: AllowMunicipalityACL()
    yield client
    client.app.dependency_overrides.pop(get_acl_user, None)


BASE_PARAMS = {
    "municipality": "GM0599",
    "start_date": "2026-01-01",
    "end_date": "2026-01-31",
    "aggregation": "day",
}


def test_statistic_is_required(stats_client):
    params = dict(BASE_PARAMS)
    response = stats_client.get("/stats_v3/statistics", params=params)
    assert response.status_code == 400
    assert "statistic" in response.json()["message"]


def test_unknown_statistic_returns_400(stats_client):
    params = dict(BASE_PARAMS, statistic="bogus")
    response = stats_client.get("/stats_v3/statistics", params=params)
    assert response.status_code == 400
    assert "statistic" in response.json()["message"]


def test_municipality_is_required(stats_client):
    params = dict(BASE_PARAMS, statistic="rentals")
    params.pop("municipality")
    response = stats_client.get("/stats_v3/statistics", params=params)
    assert response.status_code == 400
    assert "municipality" in response.json()["message"]


def test_invalid_date_range_returns_400(stats_client):
    params = dict(
        BASE_PARAMS,
        statistic="rentals",
        start_date="2026-02-01",
        end_date="2026-01-01",
    )
    response = stats_client.get("/stats_v3/statistics", params=params)
    assert response.status_code == 400
    assert response.json() == {"message": "end_date must be on or after start_date"}


def test_aggregation_is_required_for_time_series(stats_client):
    params = dict(BASE_PARAMS, statistic="rentals")
    params.pop("aggregation")
    response = stats_client.get("/stats_v3/statistics", params=params)
    assert response.status_code == 400
    assert response.json() == {"message": "aggregation is required for this statistic"}


def test_unsupported_trip_source_returns_400(stats_client):
    params = dict(
        BASE_PARAMS,
        statistic="distance_distribution",
        trip_source="trips",
    )
    params.pop("aggregation")
    response = stats_client.get("/stats_v3/statistics", params=params)
    assert response.status_code == 400
    assert "does not support trip_source" in response.json()["message"]


def test_unauthorized_municipality_returns_403(client):
    client.app.dependency_overrides[get_acl_user] = lambda: DenyMunicipalityACL()
    params = dict(BASE_PARAMS, statistic="rentals")
    response = client.get("/stats_v3/statistics", params=params)
    assert response.status_code == 403


def test_week_periods_start_on_monday_and_are_clipped():
    periods = list(iter_periods(date(2024, 1, 1), date(2024, 1, 14), "week"))
    assert periods == [
        (date(2024, 1, 1), date(2024, 1, 7)),
        (date(2024, 1, 8), date(2024, 1, 14)),
    ]

    clipped = list(iter_periods(date(2024, 1, 3), date(2024, 1, 9), "week"))
    assert clipped == [
        (date(2024, 1, 1), date(2024, 1, 7)),
        (date(2024, 1, 8), date(2024, 1, 9)),
    ]


def test_months_between_includes_all_calendar_months():
    assert months_between(date(2025, 12, 15), date(2026, 2, 2)) == [
        date(2025, 12, 1),
        date(2026, 1, 1),
        date(2026, 2, 1),
    ]


def test_bucket_aggregations():
    service = StatisticsService(None)

    assert service._aggregate_bucket(
        "max_available_vehicles",
        [{"value": 10}, {"value": 20}, {"value": 0}],
    ) == 10.0
    assert service._aggregate_bucket(
        "rentals",
        [{"value": 10}, {"value": 20}],
    ) == 30
    assert service._aggregate_bucket(
        "usage_ratio",
        [{"rentals": 10, "vehicles": 5}, {"rentals": 20, "vehicles": 5}],
    ) == 3.0
    assert service._aggregate_bucket(
        "unavailable_percentage",
        [{"percentage": 10}, {"percentage": 20}],
    ) == 15.0


def test_time_series_fills_missing_days_with_zero():
    service = StatisticsService(None)
    daily = {
        "bicycle": {
            date(2026, 1, 1): {"value": 10},
            date(2026, 1, 2): {"value": 20},
        }
    }
    operators = {"bicycle": {"lime"}}

    result = service._serialize_time_series(
        "max_available_vehicles",
        "GM0599",
        date(2026, 1, 1),
        date(2026, 1, 3),
        "day",
        "vehicles",
        daily,
        operators,
    )

    modality = result["modalities"][0]
    assert modality["modality"] == "bicycle"
    assert modality["operators"] == ["lime"]
    assert [value["value"] for value in modality["values"]] == [10, 20, 0]


def test_time_series_endpoint_returns_modalities_and_operators(stats_client):
    stats_client.app.dependency_overrides[get_conn] = lambda: FakeConn()
    params = dict(
        BASE_PARAMS,
        statistic="rentals",
        end_date="2026-01-02",
    )

    response = stats_client.get("/stats_v3/statistics", params=params)

    assert response.status_code == 200
    body = response.json()
    assert body["statistic"] == "rentals"
    assert body["aggregation"] == "day"
    assert body["trip_source"] == "vehicles"
    assert body["modalities"][0]["modality"] == "bicycle"
    assert body["modalities"][0]["operators"] == ["lime"]
    assert [value["value"] for value in body["modalities"][0]["values"]] == [10, 0]


def test_unavailable_percentage_uses_0330_total_and_daily_maximum():
    service = StatisticsService(FakeConn(UnavailableFakeCursor))

    result = service.get_time_series(
        "unavailable_percentage",
        "GM0599",
        date(2026, 1, 1),
        date(2026, 1, 1),
        "day",
        "vehicles",
    )

    modality = result["modalities"][0]
    assert modality["modality"] == "bicycle"
    assert modality["values"][0]["value"] == 20


def test_distribution_fills_missing_buckets_with_zero():
    service = StatisticsService(None)
    config = DISTRIBUTION_CONFIG[DISTANCE_DISTRIBUTION]["vehicles"]
    rows = [("bicycle", 0, 5)]
    operators = defaultdict(set, {"bicycle": {"lime"}})

    result = service._serialize_distribution(
        "distance_distribution",
        "GM0599",
        date(2026, 1, 1),
        date(2026, 1, 31),
        "vehicles",
        config,
        [date(2026, 1, 1)],
        rows,
        operators,
    )

    modality = result["modalities"][0]
    assert result["months_included"] == ["2026-01"]
    assert len(modality["buckets"]) == 10
    assert modality["buckets"][0]["category"] == 0
    assert modality["buckets"][0]["count"] == 5
    assert modality["buckets"][1]["count"] == 0
