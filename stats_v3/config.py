from typing import Literal

MAX_AVAILABLE_VEHICLES = "max_available_vehicles"
RENTALS = "rentals"
USAGE_RATIO = "usage_ratio"
UNAVAILABLE_PERCENTAGE = "unavailable_percentage"
USAGE_DISTRIBUTION = "usage_distribution"
DISTANCE_DISTRIBUTION = "distance_distribution"
REAL_DISTANCE_DISTRIBUTION = "real_distance_distribution"
DURATION_DISTRIBUTION = "duration_distribution"

Statistic = Literal[
    "max_available_vehicles",
    "rentals",
    "usage_ratio",
    "unavailable_percentage",
    "usage_distribution",
    "distance_distribution",
    "real_distance_distribution",
    "duration_distribution",
]
Aggregation = Literal["day", "week", "month"]
TripSource = Literal["vehicles", "trips"]

TIME_SERIES_STATISTICS = frozenset(
    {
        MAX_AVAILABLE_VEHICLES,
        RENTALS,
        USAGE_RATIO,
        UNAVAILABLE_PERCENTAGE,
    }
)

DISTRIBUTION_STATISTICS = frozenset(
    {
        USAGE_DISTRIBUTION,
        DISTANCE_DISTRIBUTION,
        REAL_DISTANCE_DISTRIBUTION,
        DURATION_DISTRIBUTION,
    }
)

TIME_PERIOD_LABELS = {
    0: "00:00-07:00",
    1: "07:00-09:00",
    2: "09:00-12:00",
    3: "12:00-16:00",
    4: "16:00-18:00",
    5: "18:00-24:00",
}

DISTANCE_CATEGORY_LABELS = {
    0: "0.0-0.1 km",
    1: "0.1-1 km",
    2: "1-3.7 km",
    3: "3.7-7.5 km",
    4: "7.5-15 km",
    5: "15-30 km",
    6: "30-50 km",
    7: "50-75 km",
    8: "75-500 km",
    9: ">= 500 km",
}

DURATION_CATEGORY_LABELS = {
    0: "< 1 min",
    1: "1-5 min",
    2: "5-10 min",
    3: "10-15 min",
    4: "15-20 min",
    5: "20-30 min",
    6: "30-60 min",
    7: "1-2 uur",
    8: "2 uur - 1 dag",
    9: "1 dag - 1 week",
    10: "1 week - 4 weken",
    11: ">= 4 weken",
}

DISTRIBUTION_CONFIG = {
    USAGE_DISTRIBUTION: {
        "vehicles": {
            "indicator": 14,
            "bucket_field": "time_period",
            "labels": TIME_PERIOD_LABELS,
        },
        "trips": {
            "indicator": 15,
            "bucket_field": "time_period",
            "labels": TIME_PERIOD_LABELS,
        },
    },
    DISTANCE_DISTRIBUTION: {
        "vehicles": {
            "indicator": 13,
            "bucket_field": "category",
            "labels": DISTANCE_CATEGORY_LABELS,
        },
    },
    REAL_DISTANCE_DISTRIBUTION: {
        "trips": {
            "indicator": 15,
            "bucket_field": "category",
            "labels": DISTANCE_CATEGORY_LABELS,
        },
    },
    DURATION_DISTRIBUTION: {
        "vehicles": {
            "indicator": 14,
            "bucket_field": "category",
            "labels": DURATION_CATEGORY_LABELS,
        },
    },
}

DISTRIBUTION_DEFAULT_TRIP_SOURCE = {
    USAGE_DISTRIBUTION: "vehicles",
    DISTANCE_DISTRIBUTION: "vehicles",
    REAL_DISTANCE_DISTRIBUTION: "trips",
    DURATION_DISTRIBUTION: "vehicles",
}
