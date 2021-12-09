-- Play arround with queries for aggregated stats.

SELECT system_id, DATE_TRUNC('week', date) as time_period, ROUND(AVG(value))
FROM stats_pre_process
WHERE 
date >= '2021-01-01' AND date <= '2021-12-09'
AND zone_ref = 'cbs:GM0599'
AND stat_description = 'number_of_vehicles_available'
GROUP BY system_id, DATE_TRUNC('week', date)
ORDER BY time_period;


SELECT system_id, DATE_TRUNC('month', date) as time_period, SUM(value)
FROM stats_pre_process
JOIN zones
ON stats_pre_process.zone_ref = zones.stats_ref
WHERE 
date >= '2021-01-01' AND date <= '2021-12-09'
AND zone_id IN (34234)
AND stat_description = 'number_of_trip_started'
GROUP BY system_id, DATE_TRUNC('month', date)
ORDER BY time_period;



SELECT system_id, DATE_TRUNC('month', stat_dates) as time_period, SUM(value)
FROM (
    SELECT generate_series('2021-01-01'::date, '2021-12-09'::date, '1d')::date AS stat_dates
) as t
LEFT JOIN stats_pre_process ON stat_dates = date
JOIN zones
ON stats_pre_process.zone_ref = zones.stats_ref
WHERE 
date >= '2021-01-01' AND date <= '2021-12-09'
AND zone_id IN (34234)
AND system_id in ('baqme', 'gosharing')
AND stat_description = 'number_of_trip_started'
GROUP BY DATE_TRUNC('month', stat_dates), system_id
ORDER BY time_period;


WITH 
    time_serie AS (
        SELECT generate_series('2021-01-01'::date, '2022-12-09'::date, '1d')::date AS stat_dates
    ),
    stats AS (
    SELECT system_id, DATE_TRUNC('day', stat_dates) as time_period, ROUND(AVG(value)) as value
    FROM (
        SELECT * FROM time_serie
    ) as t
    LEFT JOIN stats_pre_process ON stat_dates = date
    JOIN zones
    ON stats_pre_process.zone_ref = zones.stats_ref
    WHERE 
    date >= '2021-01-01' AND date <= '2022-12-09'
    AND zone_id IN (34234)
    AND system_id in ('baqme', 'gosharing', 'felyx', 'check')
    AND stat_description = 'number_of_vehicles_available'
    GROUP BY DATE_TRUNC('day', stat_dates), system_id
    ORDER BY time_period
    ), 
    periods AS (
        SELECT DISTINCT(DATE_TRUNC('day', stat_dates)) as time_period
        FROM time_serie
    ), 
    operators AS (
        SELECT DISTINCT(system_id) 
        FROM stats
    )
SELECT periods.time_period, operators.system_id, coalesce(stats.value, 0)
FROM periods
CROSS JOIN operators
LEFT JOIN 
stats ON
 operators.system_id = stats.system_id 
AND periods.time_period = stats.time_period 
ORDER BY periods.time_period, operators.system_id;



SELECT system_id, DATE_TRUNC('month', stat_dates) as time_period, SUM(value)
FROM (
    SELECT generate_series('2021-01-01'::date, '2021-12-09'::date, '1d')::date AS stat_dates
) as t
LEFT JOIN stats_pre_process ON stat_dates = date
JOIN zones
ON stats_pre_process.zone_ref = zones.stats_ref
WHERE 
date >= '2021-01-01' AND date <= '2021-12-09'
AND zone_id IN (34234)
AND system_id in ('baqme', 'gosharing')
AND stat_description = 'number_of_trip_started'
GROUP BY DATE_TRUNC('month', stat_dates), system_id
ORDER BY time_period;


WITH 
    time_serie AS (
        SELECT generate_series('2021-01-01'::date, '2022-12-09'::date, '1d')::date AS stat_dates
    ),
    stats AS (
    SELECT system_id, DATE_TRUNC('month', stat_dates) as time_period, SUM(value) as value
    FROM (
        SELECT * FROM time_serie
    ) as t
    LEFT JOIN stats_pre_process ON stat_dates = date
    JOIN zones
    ON stats_pre_process.zone_ref = zones.stats_ref
    WHERE 
    date >= '2021-01-01' AND date <= '2022-12-09'
    AND zone_id IN (34234)
    AND system_id in ('baqme', 'gosharing', 'felyx', 'check')
    AND stat_description = 'number_of_trip_started'
    GROUP BY DATE_TRUNC('month', stat_dates), system_id
    ORDER BY time_period
    ), 
    periods AS (
        SELECT DISTINCT(DATE_TRUNC('month', stat_dates)) as time_period
        FROM time_serie
    ), 
    operators AS (
        SELECT DISTINCT(system_id) 
        FROM stats
    )
SELECT periods.time_period, operators.system_id, coalesce(stats.value, 0)
FROM periods
CROSS JOIN operators
LEFT JOIN 
stats ON
 operators.system_id = stats.system_id 
AND periods.time_period = stats.time_period 
ORDER BY periods.time_period, operators.system_id;





SELECT x.order_date, t.sale
            x
           LEFT   JOIN sales t USING (order_date)
           ORDER  BY x.order_date;



SELECT q1.date 
            FROM
                (SELECT date_trunc('day', dd)::date as date
                FROM generate_series
                ( now()::date - %s, now()::date - 1, '1 day'::interval) dd) as q1
            WHERE q1.date NOT IN (SELECT date 
                FROM stats_pre_process
                WHERE stat_description = 'number_of_trips_ended');


SELECT t.system_id, DATE_TRUNC('month', stat_dates) as time_period, SUM(value)
FROM (
    SELECT *
        FROM 
            (
                SELECT generate_series('2021-01-01'::date, '2021-12-09'::date, '1d') AS stat_dates
            ) AS q1, 
            (
                SELECT distinct(system_id) 
                FROM stats_pre_process
                WHERE 
                date >= '2021-01-01' AND date <= '2021-12-09'
                and stat_description = 'number_of_trip_started'
            ) AS q2
) as t
LEFT JOIN stats_pre_process ON stat_dates = date
AND t.system_id = stats_pre_process.system_id
JOIN zones
ON stats_pre_process.zone_ref = zones.stats_ref
WHERE 
date >= '2021-01-01' AND date <= '2021-12-09'
AND zone_id IN (34234)
AND t.system_id = 'baqme'
AND stat_description = 'number_of_trip_started'
GROUP BY  t.system_id, DATE_TRUNC('month', stat_dates)
ORDER BY time_period;

