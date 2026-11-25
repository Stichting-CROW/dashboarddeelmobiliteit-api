from collections import defaultdict
from datetime import date
from decimal import Decimal
from typing import Dict, List, Optional, Tuple

from .config import (
    DISTRIBUTION_CONFIG,
    DISTRIBUTION_DEFAULT_TRIP_SOURCE,
    MAX_AVAILABLE_VEHICLES,
    RENTALS,
    TIME_SERIES_STATISTICS,
    UNAVAILABLE_PERCENTAGE,
    USAGE_RATIO,
)
from .time_utils import iter_days, iter_periods, months_between


def _as_number(value, digits=None):
    if isinstance(value, Decimal):
        value = float(value)
    if digits is not None:
        value = round(float(value), digits)
    if isinstance(value, float) and value.is_integer():
        return int(value)
    return value


class StatisticsService:
    def __init__(self, conn):
        self.conn = conn

    def _fetchall(self, query, params):
        cur = self.conn.cursor()
        try:
            cur.execute(query, params)
            return cur.fetchall()
        finally:
            cur.close()

    def get_geometry_refs(self, municipality: str) -> Tuple[str, ...]:
        query = """
            SELECT DISTINCT COALESCE(g.geography_id::text, z.stats_ref) AS geometry_ref
            FROM zones z
            LEFT JOIN geographies g ON g.zone_id = z.zone_id
            WHERE z.zone_type = 'municipality'
                AND z.municipality = %(municipality)s
                AND COALESCE(g.geography_id::text, z.stats_ref) IS NOT NULL
        """
        rows = self._fetchall(query, {"municipality": municipality})
        return tuple(row[0] for row in rows)

    def get_statistics(
        self,
        statistic: str,
        municipality: str,
        start_date: date,
        end_date: date,
        aggregation: Optional[str] = None,
        trip_source: Optional[str] = None,
    ) -> Dict:
        if statistic in TIME_SERIES_STATISTICS:
            if aggregation is None:
                raise ValueError("aggregation is required for this statistic")
            return self.get_time_series(
                statistic,
                municipality,
                start_date,
                end_date,
                aggregation,
                trip_source or "vehicles",
            )

        source = trip_source or DISTRIBUTION_DEFAULT_TRIP_SOURCE[statistic]
        return self.get_distribution(
            statistic,
            municipality,
            start_date,
            end_date,
            source,
        )

    def get_time_series(
        self,
        statistic: str,
        municipality: str,
        start_date: date,
        end_date: date,
        aggregation: str,
        trip_source: str,
    ) -> Dict:
        geometry_refs = self.get_geometry_refs(municipality)
        daily = defaultdict(dict)
        operators = defaultdict(set)

        if geometry_refs:
            if statistic == MAX_AVAILABLE_VEHICLES:
                daily, operators = self._get_single_indicator_daily(
                    11, trip_source, geometry_refs, start_date, end_date
                )
            elif statistic == RENTALS:
                daily, operators = self._get_single_indicator_daily(
                    9, trip_source, geometry_refs, start_date, end_date
                )
            elif statistic == USAGE_RATIO:
                daily, operators = self._get_usage_ratio_daily(
                    trip_source, geometry_refs, start_date, end_date
                )
            elif statistic == UNAVAILABLE_PERCENTAGE:
                daily, operators = self._get_unavailable_daily(
                    trip_source, geometry_refs, start_date, end_date
                )

        return self._serialize_time_series(
            statistic,
            municipality,
            start_date,
            end_date,
            aggregation,
            trip_source,
            daily,
            operators,
        )

    def get_distribution(
        self,
        statistic: str,
        municipality: str,
        start_date: date,
        end_date: date,
        trip_source: str,
    ) -> Dict:
        config_by_source = DISTRIBUTION_CONFIG.get(statistic, {})
        if trip_source not in config_by_source:
            raise ValueError(
                "statistic {} does not support trip_source {}".format(statistic, trip_source)
            )

        config = config_by_source[trip_source]
        geometry_refs = self.get_geometry_refs(municipality)
        months = months_between(start_date, end_date)
        rows = []
        operators = defaultdict(set)

        if geometry_refs:
            rows = self._get_distribution_rows(
                config, trip_source, geometry_refs, months[0], months[-1]
            )
            operators = self._get_distribution_operators(
                config, trip_source, geometry_refs, months[0], months[-1]
            )

        return self._serialize_distribution(
            statistic,
            municipality,
            start_date,
            end_date,
            trip_source,
            config,
            months,
            rows,
            operators,
        )

    def _get_single_indicator_daily(
        self,
        indicator: int,
        trip_source: str,
        geometry_refs: Tuple[str, ...],
        start_date: date,
        end_date: date,
    ):
        daily_query = """
            SELECT
                s.date AS day,
                split_part(s.vehicle_type, ':', 1) AS modality,
                SUM(s.value) AS value
            FROM day_statistics s
            WHERE s.date BETWEEN %(start_date)s AND %(end_date)s
                AND s.indicator = %(indicator)s
                AND s.trip_source = %(trip_source)s
                AND s.geometry_ref IN %(geometry_refs)s
            GROUP BY s.date, modality
        """
        operator_query = """
            SELECT DISTINCT
                split_part(s.vehicle_type, ':', 1) AS modality,
                s.system_id
            FROM day_statistics s
            WHERE s.date BETWEEN %(start_date)s AND %(end_date)s
                AND s.indicator = %(indicator)s
                AND s.trip_source = %(trip_source)s
                AND s.geometry_ref IN %(geometry_refs)s
        """
        params = {
            "start_date": start_date,
            "end_date": end_date,
            "indicator": indicator,
            "trip_source": trip_source,
            "geometry_refs": geometry_refs,
        }

        daily = defaultdict(dict)
        for day, modality, value in self._fetchall(daily_query, params):
            daily[modality][day] = {"value": value}

        operators = defaultdict(set)
        for modality, system_id in self._fetchall(operator_query, params):
            operators[modality].add(system_id)

        return daily, operators

    def _get_usage_ratio_daily(
        self,
        trip_source: str,
        geometry_refs: Tuple[str, ...],
        start_date: date,
        end_date: date,
    ):
        daily_query = """
            SELECT
                s.date AS day,
                split_part(s.vehicle_type, ':', 1) AS modality,
                SUM(
                    CASE
                        WHEN s.indicator = 9 AND s.trip_source = %(trip_source)s THEN s.value
                        ELSE 0
                    END
                ) AS rentals,
                SUM(
                    CASE
                        WHEN s.indicator = 1 AND s.trip_source IS NULL THEN s.value
                        ELSE 0
                    END
                ) AS vehicles
            FROM day_statistics s
            WHERE s.date BETWEEN %(start_date)s AND %(end_date)s
                AND s.indicator IN (1, 9)
                AND s.geometry_ref IN %(geometry_refs)s
            GROUP BY s.date, modality
        """
        operator_query = """
            SELECT DISTINCT
                split_part(s.vehicle_type, ':', 1) AS modality,
                s.system_id
            FROM day_statistics s
            WHERE s.date BETWEEN %(start_date)s AND %(end_date)s
                AND s.geometry_ref IN %(geometry_refs)s
                AND (
                    (s.indicator = 9 AND s.trip_source = %(trip_source)s)
                    OR (s.indicator = 1 AND s.trip_source IS NULL)
                )
        """
        params = {
            "start_date": start_date,
            "end_date": end_date,
            "trip_source": trip_source,
            "geometry_refs": geometry_refs,
        }

        daily = defaultdict(dict)
        for day, modality, rentals, vehicles in self._fetchall(daily_query, params):
            daily[modality][day] = {
                "rentals": rentals,
                "vehicles": vehicles,
            }

        operators = defaultdict(set)
        for modality, system_id in self._fetchall(operator_query, params):
            operators[modality].add(system_id)

        return daily, operators

    def _get_unavailable_daily(
        self,
        trip_source: str,
        geometry_refs: Tuple[str, ...],
        start_date: date,
        end_date: date,
    ):
        moment_query = """
            SELECT
                s.date AS day,
                split_part(s.vehicle_type, ':', 1) AS modality,
                SUM(CASE WHEN s.indicator = 1 THEN s.value ELSE 0 END) AS total_at_0330,
                SUM(
                    CASE
                        WHEN s.indicator = 11 AND s.trip_source = %(trip_source)s THEN s.value
                        ELSE 0
                    END
                ) AS available_at_0330
            FROM moment_statistics s
            WHERE s.date BETWEEN %(start_date)s AND %(end_date)s
                AND s.measurement_moment = 0
                AND s.indicator IN (1, 11)
                AND s.geometry_ref IN %(geometry_refs)s
            GROUP BY s.date, modality
        """
        day_total_query = """
            SELECT
                s.date AS day,
                split_part(s.vehicle_type, ':', 1) AS modality,
                SUM(s.value) AS max_total
            FROM day_statistics s
            WHERE s.date BETWEEN %(start_date)s AND %(end_date)s
                AND s.indicator = 1
                AND s.trip_source IS NULL
                AND s.geometry_ref IN %(geometry_refs)s
            GROUP BY s.date, modality
        """
        operator_query = """
            SELECT DISTINCT
                split_part(s.vehicle_type, ':', 1) AS modality,
                s.system_id
            FROM day_statistics s
            WHERE s.date BETWEEN %(start_date)s AND %(end_date)s
                AND s.geometry_ref IN %(geometry_refs)s
                AND (
                    (s.indicator = 1 AND s.trip_source IS NULL)
                    OR (s.indicator = 11 AND s.trip_source = %(trip_source)s)
                )
        """
        params = {
            "start_date": start_date,
            "end_date": end_date,
            "trip_source": trip_source,
            "geometry_refs": geometry_refs,
        }

        raw = defaultdict(dict)
        for day, modality, total_at_0330, available_at_0330 in self._fetchall(
            moment_query, params
        ):
            entry = raw[modality].setdefault(day, {})
            entry["total_at_0330"] = total_at_0330
            entry["available_at_0330"] = available_at_0330

        for day, modality, max_total in self._fetchall(day_total_query, params):
            entry = raw[modality].setdefault(day, {})
            entry["max_total"] = max_total

        daily = defaultdict(dict)
        for modality, days in raw.items():
            for day, entry in days.items():
                max_total = float(entry.get("max_total") or 0)
                if max_total <= 0:
                    percentage = 0
                else:
                    total = float(entry.get("total_at_0330") or 0)
                    available = float(entry.get("available_at_0330") or 0)
                    unavailable = max(total - available, 0)
                    percentage = unavailable / max_total * 100
                daily[modality][day] = {"percentage": percentage}

        operators = defaultdict(set)
        for modality, system_id in self._fetchall(operator_query, params):
            operators[modality].add(system_id)

        return daily, operators

    def _serialize_time_series(
        self,
        statistic: str,
        municipality: str,
        start_date: date,
        end_date: date,
        aggregation: str,
        trip_source: str,
        daily,
        operators,
    ) -> Dict:
        modality_names = sorted(set(daily.keys()) | set(operators.keys()))
        modality_results = []

        for modality in modality_names:
            values = []
            for period_start, period_end in iter_periods(
                start_date, end_date, aggregation
            ):
                entries = [
                    daily.get(modality, {}).get(
                        day, self._default_daily_entry(statistic)
                    )
                    for day in iter_days(max(period_start, start_date), period_end)
                ]
                value = self._aggregate_bucket(statistic, entries)
                values.append(
                    {
                        "period_start": period_start.isoformat(),
                        "period_end": period_end.isoformat(),
                        "value": _as_number(value, self._precision(statistic)),
                    }
                )

            modality_results.append(
                {
                    "modality": modality,
                    "operators": sorted(operators.get(modality, set())),
                    "values": values,
                }
            )

        return {
            "statistic": statistic,
            "municipality": municipality,
            "start_date": start_date.isoformat(),
            "end_date": end_date.isoformat(),
            "aggregation": aggregation,
            "trip_source": trip_source,
            "modalities": modality_results,
        }

    def _get_distribution_rows(
        self,
        config: Dict,
        trip_source: str,
        geometry_refs: Tuple[str, ...],
        start_month: date,
        end_month: date,
    ):
        bucket_field = config["bucket_field"]
        query = """
            SELECT
                split_part(s.vehicle_type, ':', 1) AS modality,
                s.{bucket_field} AS bucket,
                SUM(s.value) AS count
            FROM month_statistics s
            WHERE s.month BETWEEN %(start_month)s AND %(end_month)s
                AND s.indicator = %(indicator)s
                AND s.trip_source = %(trip_source)s
                AND s.geometry_ref IN %(geometry_refs)s
            GROUP BY modality, bucket
        """.format(bucket_field=bucket_field)
        params = {
            "start_month": start_month,
            "end_month": end_month,
            "indicator": config["indicator"],
            "trip_source": trip_source,
            "geometry_refs": geometry_refs,
        }
        return self._fetchall(query, params)

    def _get_distribution_operators(
        self,
        config: Dict,
        trip_source: str,
        geometry_refs: Tuple[str, ...],
        start_month: date,
        end_month: date,
    ):
        query = """
            SELECT DISTINCT
                split_part(s.vehicle_type, ':', 1) AS modality,
                s.system_id
            FROM month_statistics s
            WHERE s.month BETWEEN %(start_month)s AND %(end_month)s
                AND s.indicator = %(indicator)s
                AND s.trip_source = %(trip_source)s
                AND s.geometry_ref IN %(geometry_refs)s
        """
        params = {
            "start_month": start_month,
            "end_month": end_month,
            "indicator": config["indicator"],
            "trip_source": trip_source,
            "geometry_refs": geometry_refs,
        }
        operators = defaultdict(set)
        for modality, system_id in self._fetchall(query, params):
            operators[modality].add(system_id)
        return operators

    def _serialize_distribution(
        self,
        statistic: str,
        municipality: str,
        start_date: date,
        end_date: date,
        trip_source: str,
        config: Dict,
        months: List[date],
        rows,
        operators,
    ) -> Dict:
        bucket_field = config["bucket_field"]
        labels = config["labels"]

        data = defaultdict(dict)
        for modality, bucket, count in rows:
            data[modality][int(bucket)] = count

        modality_names = sorted(set(data.keys()) | set(operators.keys()))
        modality_results = []

        for modality in modality_names:
            buckets = []
            for index in sorted(labels.keys()):
                count = data.get(modality, {}).get(index, 0)
                buckets.append(
                    {
                        bucket_field: index,
                        "label": labels[index],
                        "count": _as_number(count, 0),
                    }
                )
            modality_results.append(
                {
                    "modality": modality,
                    "operators": sorted(operators.get(modality, set())),
                    "buckets": buckets,
                }
            )

        return {
            "statistic": statistic,
            "municipality": municipality,
            "start_date": start_date.isoformat(),
            "end_date": end_date.isoformat(),
            "trip_source": trip_source,
            "months_included": [month.strftime("%Y-%m") for month in months],
            "modalities": modality_results,
        }

    def _default_daily_entry(self, statistic: str) -> Dict:
        if statistic == USAGE_RATIO:
            return {"rentals": 0, "vehicles": 0}
        if statistic == UNAVAILABLE_PERCENTAGE:
            return {"percentage": 0}
        return {"value": 0}

    def _aggregate_bucket(self, statistic: str, entries: List[Dict]):
        if not entries:
            return 0

        if statistic == MAX_AVAILABLE_VEHICLES:
            return sum(float(entry["value"]) for entry in entries) / len(entries)

        if statistic == RENTALS:
            return sum(float(entry["value"]) for entry in entries)

        if statistic == USAGE_RATIO:
            rentals = sum(float(entry["rentals"]) for entry in entries)
            vehicles = sum(float(entry["vehicles"]) for entry in entries)
            if vehicles <= 0:
                return 0
            return rentals / vehicles

        if statistic == UNAVAILABLE_PERCENTAGE:
            return sum(float(entry["percentage"]) for entry in entries) / len(entries)

        return 0

    def _precision(self, statistic: str) -> int:
        if statistic == USAGE_RATIO:
            return 2
        if statistic == UNAVAILABLE_PERCENTAGE:
            return 1
        if statistic == RENTALS:
            return 0
        return 2
