UNAUTHORIZED_MESSAGE = "You are not authorized (no token or invalid token is present)."


def test_protected_endpoint_without_token_returns_legacy_401_shape(client):
    response = client.get("/trips")
    assert response.status_code == 401
    assert response.json() == {"code": 401, "message": UNAUTHORIZED_MESSAGE}


def test_anonymous_consumer_header_is_rejected(client):
    response = client.get("/trips", headers={"X-Consumer-Username": "anonymous"})
    assert response.status_code == 401
    assert response.json()["message"] == UNAUTHORIZED_MESSAGE


def test_malformed_bearer_token_is_rejected(client):
    response = client.get("/trips", headers={"Authorization": "Bearer not-a-jwt"})
    assert response.status_code == 401
    assert response.json()["message"] == UNAUTHORIZED_MESSAGE


def test_unknown_user_is_rejected_with_dummy_conn(client, monkeypatch):
    class StubAccessControl:
        def retrieve_acl_user(self, username, conn):
            return None

    monkeypatch.setattr("core.security.access_control.AccessControl", StubAccessControl)

    import jwt as pyjwt

    token = pyjwt.encode({"email": "user@example.com"}, "secret", algorithm="HS256")
    response = client.get("/trips", headers={"Authorization": "Bearer " + token})
    assert response.status_code == 401
