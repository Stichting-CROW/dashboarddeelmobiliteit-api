from core.security import get_acl_user


def test_public_zones_requires_filter(client):
    response = client.get("/public/zones")
    assert response.status_code == 400
    assert response.json() == {"message": "No gm_code (deprecated) or zone_ids, municipalities."}


def test_zones_requires_filter(client):
    response = client.get("/zones")
    assert response.status_code == 400
    assert response.json() == {"message": "No gm_code (deprecated), zone_ids or municipalities."}


def test_invalid_aggregation_level_returns_400(client):
    client.app.dependency_overrides[get_acl_user] = lambda: None

    params = {
        "start_time": "2023-01-01T00:00:00Z",
        "end_time": "2023-02-01T00:00:00Z",
        "aggregation_level": "bogus",
        "aggregation_function": "MIN",
        "group_by": "operator",
    }
    response = client.get("/stats_v2/availability_stats", params=params)
    assert response.status_code == 400
    body = response.json()
    assert "message" in body


def test_missing_aggregation_function_returns_400(client):
    client.app.dependency_overrides[get_acl_user] = lambda: None

    params = {
        "start_time": "2023-01-01T00:00:00Z",
        "end_time": "2023-02-01T00:00:00Z",
        "aggregation_level": "15m",
        "group_by": "operator",
    }
    response = client.get("/stats_v2/availability_stats", params=params)
    assert response.status_code == 400
    assert "message" in response.json()


def test_invalid_timestamp_format_returns_400(client):
    params = {"start_time": "01-01-2023", "end_time": "2023-02-01T00:00:00Z"}
    response = client.get("/public/park_events/stats", params=params)
    assert response.status_code == 400
    assert "message" in response.json()
