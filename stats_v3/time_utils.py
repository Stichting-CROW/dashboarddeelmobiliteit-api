from datetime import date, timedelta
from typing import Iterator, List, Tuple


def add_month(value: date) -> date:
    if value.month == 12:
        return date(value.year + 1, 1, 1)
    return date(value.year, value.month + 1, 1)


def start_of_month(value: date) -> date:
    return date(value.year, value.month, 1)


def iter_days(start_date: date, end_date: date) -> Iterator[date]:
    current = start_date
    while current <= end_date:
        yield current
        current += timedelta(days=1)


def iter_periods(start_date: date, end_date: date, aggregation: str) -> Iterator[Tuple[date, date]]:
    if aggregation == "day":
        for day in iter_days(start_date, end_date):
            yield day, day
        return

    if aggregation == "week":
        bucket_start = start_date - timedelta(days=start_date.weekday())
        while bucket_start <= end_date:
            bucket_end = bucket_start + timedelta(days=6)
            period_end = min(bucket_end, end_date)
            if bucket_start <= end_date and period_end >= start_date:
                yield bucket_start, period_end
            bucket_start += timedelta(days=7)
        return

    if aggregation == "month":
        bucket_start = start_of_month(start_date)
        while bucket_start <= end_date:
            next_month = add_month(bucket_start)
            bucket_end = next_month - timedelta(days=1)
            period_end = min(bucket_end, end_date)
            if bucket_start <= end_date and period_end >= start_date:
                yield bucket_start, period_end
            bucket_start = next_month
        return

    raise ValueError("invalid aggregation: {}".format(aggregation))


def months_between(start_date: date, end_date: date) -> List[date]:
    months = []
    current = start_of_month(start_date)
    last = start_of_month(end_date)
    while current <= last:
        months.append(current)
        current = add_month(current)
    return months
