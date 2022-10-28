class AvailabilityStats():
    def converted_aggregation_level(self, aggregation_level):
        allowed_aggregation_levels = {
            '15m': '15 minutes',
            'hour': '1 hour',
            'day': '1 day',
            'week': '1 week',
            'month': '1 month'
        }
        return allowed_aggregation_levels[aggregation_level];

    def get_params(self, d_filter, agg_level):
        return {
            "agg_level": agg_level,
            "zone_ids": d_filter.get_zones()
        }

    def get_availability_stats(self, conn, d_filter, aggregation_level, group_by):
        if group_by == 'modality':
            return self.get_availability_stats_per_modality(conn, d_filter, aggregation_level)
        elif group_by == 'operator':
            return self.get_availability_stats_per_operator(conn, d_filter, aggregation_level)

    def get_availability_stats_per_modality(self, conn, d_filter, aggregation_level):
        agg_level = self.converted_aggregation_level(aggregation_level);

        query = """
            SELECT
                modality,
                time_bucket(%(agg_level)s, time) AS bucket,
                MAX(number_of_vehicles_parked) as amount
            FROM stats_number_of_vehicles_parked
            WHERE
                time > NOW() - INTERVAL '1 hours'
                AND zone_id IN (%(zone_ids)s)
            GROUP BY bucket, modality
            ORDER BY bucket ASC, modality
        """

        params = self.get_params(d_filter, aggregation_level)

        cur = conn.cursor()
        cur.execute(query, params)
        rows = cur.fetchall()
        conn.commit()

        result = {}
        result["aggregation_level"] = agg_level
        result["values"] = rows
        return result

    def get_availability_stats_per_operator(self, conn, d_filter, aggregation_level):
        agg_level = self.converted_aggregation_level(aggregation_level);

        query = """
            SELECT
                system_id,
                time_bucket(%(agg_level)s, time) AS bucket,
                MAX(number_of_vehicles_parked) as amount
            FROM stats_number_of_vehicles_parked
            WHERE
                time > NOW() - INTERVAL '1 hours'
                AND zone_id IN (%(zone_ids)s)
            GROUP BY bucket, system_id
            ORDER BY bucket ASC, system_id
        """

        params = self.get_params(d_filter, aggregation_level)

        cur = conn.cursor()
        cur.execute(query, params)
        rows = cur.fetchall()
        conn.commit()

        result = {}
        result["aggregation_level"] = agg_level
        result["values"] = rows
        return result

    def get_stats(self, conn, d_filter, aggregation_level):
        stmt = """
        WITH 
            time_serie AS (
                SELECT generate_series(%(start_date)s::date, %(end_date)s::date, '1d')::date AS stat_dates
            ),
            stats AS (
            SELECT system_id, DATE_TRUNC(%(aggregation)s, stat_dates) as time_period, ROUND(AVG(value)) as value
            FROM (
                SELECT * FROM time_serie
            ) as t
            LEFT JOIN stats_pre_process ON stat_dates = date
            JOIN zones
            ON stats_pre_process.zone_ref = zones.stats_ref
            WHERE 
            date >= %(start_date)s AND date <= %(end_date)s
            AND (false = %(filter_zone_id)s or zone_id IN %(zone_ids)s)
            AND (false = %(filter_system_id)s or system_id IN %(system_ids)s)
            AND stat_description = 'number_of_vehicles_available'
            GROUP BY DATE_TRUNC(%(aggregation)s, stat_dates), system_id
            ORDER BY time_period
            ), 
            periods AS (
                SELECT DISTINCT(DATE_TRUNC(%(aggregation)s, stat_dates)) as time_period
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
        WHERE operators.system_id is not null and operators.system_id != ''
        ORDER BY periods.time_period, operators.system_id"""
        params = {
            "start_date": d_filter.get_start_time(),
            "end_date": d_filter.get_end_time(),
            "aggregation": aggregation_level,
            "filter_zone_id": d_filter.has_zone_filter(), 
            "zone_ids":  d_filter.get_zones(),
            "filter_system_id": d_filter.has_operator_filter(),
            "system_ids": d_filter.get_operators()
        }
        cur = conn.cursor()
        cur.execute(stmt, params)
        rows = cur.fetchall()
        conn.commit()
        
        result = {}
        result["aggregation_level"] = aggregation_level
        result["values"] = self.serialize_values(rows)
        return result

    def serialize_values(self, rows):
        result = []
        start_interval = None
        record = {}
        first = True
        for row in rows:
            if row[0] != start_interval:
                if not first:
                    result.append(record)
                start_interval = row[0]
                record = {}
                record["start_interval"] = str(start_interval)
                first = False
            record[row[1]] = int(row[2])
        return result