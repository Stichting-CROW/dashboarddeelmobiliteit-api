import pytest
from fastapi.testclient import TestClient

import main
from core.database import get_conn, get_timescaledb_conn


@pytest.fixture
def app():
    return main.app


@pytest.fixture
def client(app):
    def _dummy_conn():
        yield object()

    app.dependency_overrides[get_conn] = _dummy_conn
    app.dependency_overrides[get_timescaledb_conn] = _dummy_conn
    yield TestClient(app)
    app.dependency_overrides.clear()
