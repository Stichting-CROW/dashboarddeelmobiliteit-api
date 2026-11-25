# Stats v3 frontend integration guide

This guide describes the statistics endpoint added for the new dashboard graphs.
The endpoint returns one statistic per request, so charts can be loaded in
parallel with independent requests.

## Endpoint

```http
GET /stats_v3/statistics
```

In the deployed environment this is normally reached through the gateway, for
example:

```text
https://api.example.nl/dashboard-api/stats_v3/statistics
```

## Authentication

The endpoint uses the same authentication as the other dashboard API endpoints.
A valid token is required. A user can only request a municipality they have
access to.

Possible authentication/authorisation responses:

| Status | Meaning |
| --- | --- |
| `401` | No or invalid token. |
| `403` | The user is not allowed to access the requested municipality. |

## Query parameters

| Parameter | Required | Description |
| --- | --- | --- |
| `statistic` | Yes | One of the statistic names listed below. |
| `municipality` | Yes | Municipality GM code, for example `GM0599`. |
| `start_date` | Yes | Inclusive start date, format `YYYY-MM-DD`. |
| `end_date` | Yes | Inclusive end date, format `YYYY-MM-DD`. |
| `aggregation` | Time-series statistics only | `day`, `week` or `month`. Required for time-series statistics and ignored for distributions. |
| `trip_source` | No | `vehicles` or `trips`. Default depends on the statistic, usually `vehicles`. |

### Statistic names

Time-series statistics:

- `max_available_vehicles`
- `rentals`
- `usage_ratio`
- `unavailable_percentage`

Distribution statistics:

- `usage_distribution`
- `distance_distribution`
- `real_distance_distribution`
- `duration_distribution`

## Time-series response

Time-series statistics return one value per period per modality.

```json
{
  "statistic": "rentals",
  "municipality": "GM0599",
  "start_date": "2026-01-01",
  "end_date": "2026-03-31",
  "aggregation": "week",
  "trip_source": "vehicles",
  "modalities": [
    {
      "modality": "bicycle",
      "operators": ["bolt", "lime"],
      "values": [
        {
          "period_start": "2026-01-05",
          "period_end": "2026-01-11",
          "value": 1234
        },
        {
          "period_start": "2026-01-12",
          "period_end": "2026-01-18",
          "value": 1180
        }
      ]
    }
  ]
}
```

### Period rules

- `week` buckets always start on Monday.
- `month` buckets always start on the first day of the month.
- The first `period_start` can be before the requested `start_date` for weekly
  or monthly aggregation. Only days inside the requested date range contribute
  to the value.
- `period_end` is clipped to the requested `end_date`.
- `start_date` and `end_date` are inclusive.

### Missing data

- A missing day inside the requested range counts as `0`.
- A modality is included when it has data or operators in the requested range.
  If there is no data at all, `modalities` is an empty array.
- `operators` contains the operator/system ids that have data for that modality
  anywhere in the requested range. It is not a per-period breakdown.

## Time-series statistic definitions

| Statistic | Meaning | Week/month rollup |
| --- | --- | --- |
| `max_available_vehicles` | Daily maximum number of available vehicles, based on the six measurement moments per day. | Average of the daily maxima. |
| `rentals` | Number of rentals per day. | Sum of daily rentals. |
| `usage_ratio` | Rentals per vehicle per day. | Total rentals divided by total daily vehicle count. |
| `unavailable_percentage` | Percentage of vehicles that was not available at 03:30. | Average of the daily percentages. |

### `unavailable_percentage` calculation

For each day and modality:

```text
not_available_0330 = max(total_at_0330 - available_at_0330, 0)
daily_percentage = not_available_0330 / max_daily_total * 100
```

Where:

- `total_at_0330` is the total number of vehicles in public space at 03:30.
- `available_at_0330` is the number of available vehicles at 03:30.
- `max_daily_total` is the maximum number of vehicles in public space on that
  day across the six measurement moments.
- If the denominator is missing or zero, the daily value is `0`.
- The numerator is clamped at zero to avoid negative percentages.

### Numeric precision

| Statistic | Value format |
| --- | --- |
| `max_available_vehicles` | Count, rounded to 2 decimals when needed. |
| `rentals` | Integer count. |
| `usage_ratio` | Rounded to 2 decimals. |
| `unavailable_percentage` | Rounded to 1 decimal. |

## Distribution response

Distribution statistics return counts per bucket per modality, summed over all
included months.

```json
{
  "statistic": "usage_distribution",
  "municipality": "GM0599",
  "start_date": "2026-01-01",
  "end_date": "2026-03-31",
  "trip_source": "vehicles",
  "months_included": ["2026-01", "2026-02", "2026-03"],
  "modalities": [
    {
      "modality": "bicycle",
      "operators": ["bolt", "lime"],
      "buckets": [
        {
          "time_period": 0,
          "label": "00:00-07:00",
          "count": 120
        },
        {
          "time_period": 1,
          "label": "07:00-09:00",
          "count": 430
        }
      ]
    }
  ]
}
```

### Distribution rules

- The source data is monthly. The response covers all full calendar months that
  intersect `start_date..end_date`.
- `months_included` explicitly lists those months as `YYYY-MM`.
- Counts are raw counts, not percentages. Calculate percentages in the frontend
  if the chart needs them.
- Every defined bucket is returned for every included modality. Missing buckets
  have `count: 0`.
- `operators` contains the operators/system ids with data for that modality in
  the included months.

### Distribution bucket fields

| Statistic | Bucket field | Bucket range |
| --- | --- | --- |
| `usage_distribution` | `time_period` | `0` to `5` |
| `distance_distribution` | `category` | `0` to `9` |
| `real_distance_distribution` | `category` | `0` to `9` |
| `duration_distribution` | `category` | `0` to `11` |

## Bucket labels

### Usage over the day (`time_period`)

| Value | Label |
| --- | --- |
| `0` | `00:00-07:00` |
| `1` | `07:00-09:00` |
| `2` | `09:00-12:00` |
| `3` | `12:00-16:00` |
| `4` | `16:00-18:00` |
| `5` | `18:00-24:00` |

The periods are based on the rental end time in the `Europe/Amsterdam` timezone.

### Distance (`category`)

Used by `distance_distribution` and `real_distance_distribution`.

| Value | Label |
| --- | --- |
| `0` | `0.0-0.1 km` |
| `1` | `0.1-1 km` |
| `2` | `1-3.7 km` |
| `3` | `3.7-7.5 km` |
| `4` | `7.5-15 km` |
| `5` | `15-30 km` |
| `6` | `30-50 km` |
| `7` | `50-75 km` |
| `8` | `75-500 km` |
| `9` | `>= 500 km` |

`distance_distribution` is the crow-flies distance.  
`real_distance_distribution` is the recorded distance over the road.

### Rental duration (`category`)

Used by `duration_distribution`.

| Value | Label |
| --- | --- |
| `0` | `< 1 min` |
| `1` | `1-5 min` |
| `2` | `5-10 min` |
| `3` | `10-15 min` |
| `4` | `15-20 min` |
| `5` | `20-30 min` |
| `6` | `30-60 min` |
| `7` | `1-2 uur` |
| `8` | `2 uur - 1 dag` |
| `9` | `1 dag - 1 week` |
| `10` | `1 week - 4 weken` |
| `11` | `>= 4 weken` |

## `trip_source` support

| Statistic | Default | Supported values |
| --- | --- | --- |
| `max_available_vehicles` | `vehicles` | `vehicles`, `trips` |
| `rentals` | `vehicles` | `vehicles`, `trips` |
| `usage_ratio` | `vehicles` | `vehicles`, `trips` |
| `unavailable_percentage` | `vehicles` | `vehicles`, `trips` |
| `usage_distribution` | `vehicles` | `vehicles`, `trips` |
| `distance_distribution` | `vehicles` | `vehicles` only |
| `real_distance_distribution` | `trips` | `trips` only |
| `duration_distribution` | `vehicles` | `vehicles` only |

For `real_distance_distribution`, omitting `trip_source` automatically uses
`trips`. Requesting an unsupported source returns `400`.

## Modalities and operators

- `modality` is the vehicle form factor, for example `bicycle`, `moped`, `car`
  or `cargo_bicycle`.
- Multiple vehicle types with the same modality are combined. For example,
  electric and non-electric bicycles both contribute to `bicycle`.
- `operators` is a list of operator/system ids such as `lime` or `bolt`.
- If a chart combines modalities, sum the values per period in the frontend.

## Error handling

Validation and request errors return `400` with a JSON body:

```json
{
  "message": "aggregation is required for this statistic"
}
```

Common errors:

| Situation | Response |
| --- | --- |
| Missing or unknown `statistic` | `400` |
| Missing `municipality`, `start_date` or `end_date` | `400` |
| Invalid date format | `400` |
| `end_date` before `start_date` | `400` |
| Missing `aggregation` for a time-series statistic | `400` |
| Unsupported `trip_source` for a distribution | `400` |
| User not allowed to access the municipality | `403` |
| No or invalid token | `401` |

## Examples

### Rentals per week

```http
GET /stats_v3/statistics
    ?statistic=rentals
    &municipality=GM0599
    &start_date=2026-01-01
    &end_date=2026-03-31
    &aggregation=week
```

### Maximum available vehicles per day

```http
GET /stats_v3/statistics
    ?statistic=max_available_vehicles
    &municipality=GM0599
    &start_date=2026-01-01
    &end_date=2026-01-31
    &aggregation=day
```

### Unavailable percentage per month

```http
GET /stats_v3/statistics
    ?statistic=unavailable_percentage
    &municipality=GM0599
    &start_date=2026-01-01
    &end_date=2026-12-31
    &aggregation=month
```

### Usage distribution over the day

```http
GET /stats_v3/statistics
    ?statistic=usage_distribution
    &municipality=GM0599
    &start_date=2026-01-01
    &end_date=2026-03-31
```

`aggregation` is not needed for distributions.

### Real distance distribution

```http
GET /stats_v3/statistics
    ?statistic=real_distance_distribution
    &municipality=GM0599
    &start_date=2026-01-01
    &end_date=2026-03-31
    &trip_source=trips
```

`trip_source=trips` may be omitted here because it is the default for this
statistic.

## Suggested TypeScript types

```ts
export type Statistic =
  | "max_available_vehicles"
  | "rentals"
  | "usage_ratio"
  | "unavailable_percentage"
  | "usage_distribution"
  | "distance_distribution"
  | "real_distance_distribution"
  | "duration_distribution";

export type Aggregation = "day" | "week" | "month";
export type TripSource = "vehicles" | "trips";

export interface TimeSeriesValue {
  period_start: string;
  period_end: string;
  value: number;
}

export interface DistributionBucket {
  time_period?: number;
  category?: number;
  label: string;
  count: number;
}

export interface ModalityTimeSeries {
  modality: string;
  operators: string[];
  values: TimeSeriesValue[];
}

export interface ModalityDistribution {
  modality: string;
  operators: string[];
  buckets: DistributionBucket[];
}

export interface TimeSeriesResponse {
  statistic: Statistic;
  municipality: string;
  start_date: string;
  end_date: string;
  aggregation: Aggregation;
  trip_source: TripSource;
  modalities: ModalityTimeSeries[];
}

export interface DistributionResponse {
  statistic: Statistic;
  municipality: string;
  start_date: string;
  end_date: string;
  trip_source: TripSource;
  months_included: string[];
  modalities: ModalityDistribution[];
}
```

## Backend implementation reference

- Route: `routers/statistics.py`
- Statistic definitions and labels: `stats_v3/config.py`
- Query and response logic: `stats_v3/service.py`
- Period/month helpers: `stats_v3/time_utils.py`
- Tests: `tests/test_statistics.py`
