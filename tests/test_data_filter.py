import data_filter
from core.filter_params import get_data_filter


def test_empty_params_produce_default_filter():
    d_filter = get_data_filter()
    assert d_filter.get_zones() == (-1,)
    assert d_filter.get_operators() == ("undefined",)
    assert d_filter.get_municipalities() == ("undefined",)
    assert not d_filter.has_zone_filter()
    assert not d_filter.has_operator_filter()
    assert not d_filter.has_municipalities()
    assert d_filter.get_start_time() is None
    assert d_filter.get_end_time() is None
    assert d_filter.get_gmcode() is None
    assert d_filter.get_trip_source() == "vehicles"


def test_trip_source_can_be_overridden():
    d_filter = data_filter.DataFilter.build({"trip_source": "trips"})
    assert d_filter.get_trip_source() == "trips"


def test_comma_separated_lists_are_split():
    d_filter = data_filter.DataFilter.build({
        "zone_ids": "51748,51749",
        "operators": "lime,felyx",
        "municipalities": "GM0344,GM0014",
        "form_factors": "bicycle,moped",
    })
    assert d_filter.get_zones() == ("51748", "51749")
    assert d_filter.get_operators() == ("lime", "felyx")
    assert d_filter.get_municipalities() == ("GM0344", "GM0014")
    assert d_filter.get_form_factors() == ("bicycle", "moped")
    assert d_filter.has_zone_filter()
    assert d_filter.has_operator_filter()
    assert d_filter.has_form_factor_filter()


def test_gm_code_adds_municipality():
    d_filter = data_filter.DataFilter.build({"gm_code": "GM0344"})
    assert d_filter.get_gmcode() == "GM0344"
    assert d_filter.has_municipalities()
    assert d_filter.get_municipalities() == ("GM0344",)


def test_timestamp_is_parsed_as_utc_datetime():
    import datetime

    d_filter = data_filter.DataFilter.build({"timestamp": "2023-09-19T00:00:00Z"})
    expected = datetime.datetime(2023, 9, 19, tzinfo=datetime.timezone.utc)
    assert d_filter.get_timestamp() == expected
    assert d_filter.has_timestamp()


def test_include_unknown_form_factors():
    d_filter = data_filter.DataFilter.build({"form_factors": "bicycle,unknown"})
    assert d_filter.include_unknown_form_factors()

    other = data_filter.DataFilter.build({"form_factors": "bicycle"})
    assert not other.include_unknown_form_factors()


def test_is_historical_based_on_end_time():
    import datetime

    def fmt(dt):
        return dt.strftime("%Y-%m-%dT%H:%M:%SZ")

    now = datetime.datetime.now(datetime.timezone.utc)
    recent = data_filter.DataFilter.build({"end_time": fmt(now - datetime.timedelta(hours=1))})
    old = data_filter.DataFilter.build({"end_time": fmt(now - datetime.timedelta(days=5))})

    assert not recent.is_historical()
    assert old.is_historical()
    assert not data_filter.DataFilter.build({}).is_historical()


def test_add_filters_based_on_acl():
    class FakeACL:
        organisation_type = "OPERATOR"
        operator_filters = {"lime"}
        zone_filters = {"1234"}

    d_filter = data_filter.DataFilter.build({})
    d_filter.add_filters_based_on_acl(FakeACL())
    assert d_filter.get_operators() == ("lime",)
    assert d_filter.get_zones() == ("1234",)

    admin = data_filter.DataFilter.build({})

    class FakeAdmin(FakeACL):
        organisation_type = "ADMIN"

    admin.add_filters_based_on_acl(FakeAdmin())
    assert admin.get_operators() == ("undefined",)
