from datetime import datetime, timezone

import pytest

from core.security import get_acl_user
from data_quality import rejections

VALID_BODY = {
    "system_id": "lime",
    "start_time": "2026-11-01T08:00:00Z",
    "end_time": "2026-11-01T12:00:00Z",
    "municipalities": ["GM0599"],
    "vehicle_types": "all",
    "data_types": ["aanbod", "verhuringen"],
    "issue_owner": "operator",
    "note": " Feed was empty ",
}

DB_ROW = (
    7, None, datetime(2026, 11, 1, 8, tzinfo=timezone.utc), None, None, ["moped"],
    ["vehicles", "service_areas"], "dashboard_deelmobiliteit", "", "admin@example.com",
    datetime(2026, 11, 1, 9, tzinfo=timezone.utc), None, None,
)


class FakeACL:
    def __init__(self, organisation_type="ADMIN", operators=(), municipalities=()):
        self.username = "admin@example.com"
        self.organisation_type = organisation_type
        self.operator_filters = set(operators)
        self.municipality_filters = set(municipalities)

    def check_municipality_code(self, municipality_code):
        if not self.municipality_filters or municipality_code in self.municipality_filters:
            return True, None
        return False, "User is not allowed to access this municipality."


@pytest.fixture
def as_user(client):
    def _as_user(acl):
        client.app.dependency_overrides[get_acl_user] = lambda: acl
        return client

    yield _as_user
    client.app.dependency_overrides.pop(get_acl_user, None)


@pytest.fixture
def calls(monkeypatch):
    recorded = {}

    def record(name, result):
        def _fn(*args):
            recorded[name] = args
            return result(*args) if callable(result) else result
        return _fn

    monkeypatch.setattr(rejections, "list_rejections", record("list", []))
    monkeypatch.setattr(rejections, "create_rejection", record(
        "create", lambda conn, body, username: rejections._from_row(DB_ROW)))
    monkeypatch.setattr(rejections, "update_rejection", record("update", None))
    monkeypatch.setattr(rejections, "delete_rejection", record("delete", True))
    return recorded


def test_maps_database_row_to_api_values():
    result = rejections._from_row(DB_ROW).model_dump()
    assert result["system_id"] == "all"
    assert result["municipalities"] == "all"
    assert result["vehicle_types"] == ["moped"]
    assert result["data_types"] == ["aanbod", "servicegebieden"]


def test_maps_api_input_to_database_values():
    body = rejections.ManualRejectionInput(**{
        **VALID_BODY, "system_id": "all", "municipalities": "all",
    })
    params = rejections._to_db_params(body)
    assert params["system_id"] is None
    assert params["municipalities"] is None
    assert params["modalities"] is None
    assert params["data_types"] == ["vehicles", "trips"]
    assert params["note"] == "Feed was empty"


def test_operator_accounts_only_get_their_own_and_all_operator_rejections(as_user, calls):
    client = as_user(FakeACL("OPERATOR", operators={"lime", "dott"}))
    response = client.get("/data_quality/rejections?start_date=2026-11-01&end_date=2026-11-07")
    assert response.status_code == 200
    assert calls["list"][4] == ["dott", "lime"]


def test_admins_get_rejections_for_every_operator(as_user, calls):
    client = as_user(FakeACL())
    response = client.get(
        "/data_quality/rejections?start_date=2026-11-01&end_date=2026-11-07&gm_code=GM0599"
    )
    assert response.status_code == 200
    assert calls["list"][3] == "GM0599"
    assert calls["list"][4] is None


def test_municipality_users_cannot_read_other_municipalities(as_user, calls):
    client = as_user(FakeACL("MUNICIPALITY", municipalities={"GM0363"}))
    response = client.get(
        "/data_quality/rejections?start_date=2026-11-01&end_date=2026-11-07&gm_code=GM0599"
    )
    assert response.status_code == 403
    assert "list" not in calls


def test_rejects_end_date_before_start_date(as_user, calls):
    client = as_user(FakeACL())
    response = client.get("/data_quality/rejections?start_date=2026-11-07&end_date=2026-11-01")
    assert response.status_code == 400


@pytest.mark.parametrize("method,path", [
    ("post", "/data_quality/rejections"),
    ("put", "/data_quality/rejections/1"),
    ("delete", "/data_quality/rejections/1"),
])
def test_only_admins_can_change_rejections(as_user, calls, method, path):
    client = as_user(FakeACL("MUNICIPALITY", municipalities={"GM0599"}))
    kwargs = {} if method == "delete" else {"json": VALID_BODY}
    response = getattr(client, method)(path, **kwargs)
    assert response.status_code == 403
    assert not calls


def test_admin_creates_rejection_with_username_from_token(as_user, calls):
    client = as_user(FakeACL())
    response = client.post("/data_quality/rejections", json=VALID_BODY)
    assert response.status_code == 201
    assert calls["create"][2] == "admin@example.com"
    assert response.json()["rejection_id"] == 7


@pytest.mark.parametrize("override", [
    {"end_time": "2026-11-01T07:00:00Z"},
    {"system_id": " "},
    {"municipalities": []},
    {"data_types": []},
    {"data_types": ["vehicles"]},
    {"issue_owner": "someone"},
    {"start_time": "2026-11-01T08:00:00"},
])
def test_validates_rejection_input(as_user, calls, override):
    client = as_user(FakeACL())
    response = client.post("/data_quality/rejections", json={**VALID_BODY, **override})
    assert response.status_code == 400
    assert "create" not in calls


def test_update_of_unknown_rejection_returns_404(as_user, calls):
    client = as_user(FakeACL())
    response = client.put("/data_quality/rejections/99", json=VALID_BODY)
    assert response.status_code == 404


def test_delete_returns_204(as_user, calls):
    client = as_user(FakeACL())
    response = client.delete("/data_quality/rejections/3")
    assert response.status_code == 204
    assert calls["delete"][1:] == (3, "admin@example.com")
