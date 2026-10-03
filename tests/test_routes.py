from fastapi.routing import APIRoute

EXPECTED_ROUTES = {
    ("GET", "/area"),
    ("GET", "/trips"),
    ("GET", "/trips/stats"),
    ("GET", "/v2/trips/origins"),
    ("GET", "/v2/trips/destinations"),
    ("GET", "/rentals"),
    ("GET", "/rentals/stats"),
    ("GET", "/zones"),
    ("DELETE", "/zone/{zone_id}"),
    ("PUT", "/zone"),
    ("POST", "/zone"),
    ("GET", "/public/zones"),
    ("GET", "/public/municipalities"),
    ("GET", "/public/vehicles_in_public_space"),
    ("GET", "/public/filters"),
    ("GET", "/public/get_municipality_based_on_latlng"),
    ("GET", "/public/active_feeds"),
    ("GET", "/park_events"),
    ("GET", "/v2/park_events/stats"),
    ("GET", "/public/park_events/stats"),
    ("POST", "/parkeertelling"),
    ("GET", "/stats/generate_report"),
    ("GET", "/raw_data"),
    ("GET", "/menu/acl"),
    ("GET", "/aggregated_stats/available_vehicles"),
    ("GET", "/aggregated_stats/rentals"),
    ("GET", "/stats_v2/availability_stats"),
    ("GET", "/stats_v2/rental_stats"),
}

FRAMEWORK_ROUTES = {"/docs", "/docs/oauth2-redirect", "/openapi.json", "/redoc"}


def collect_routes(app):
    routes = set()
    for route in app.routes:
        if isinstance(route, APIRoute) and route.path not in FRAMEWORK_ROUTES:
            for method in route.methods:
                routes.add((method, route.path))
    return routes


def test_all_flask_routes_are_present(app):
    actual = collect_routes(app)
    missing = EXPECTED_ROUTES - actual
    assert not missing, "Missing routes: {}".format(missing)


def test_no_unexpected_extra_routes(app):
    actual = collect_routes(app)
    extra = actual - EXPECTED_ROUTES
    assert not extra, "Unexpected routes: {}".format(extra)


def test_openapi_schema_is_valid(app):
    schema = app.openapi()
    paths = set(schema["paths"].keys())
    expected_paths = {path for _, path in EXPECTED_ROUTES}
    assert expected_paths == paths
