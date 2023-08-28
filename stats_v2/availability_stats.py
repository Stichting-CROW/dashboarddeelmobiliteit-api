from psycopg2 import sql

class AvailabilityStats():
    def converted_aggregation_level(self, aggregation_level):
        allowed_aggregation_levels = {
            '5m': '5 minutes',
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
            "zone_ids": d_filter.get_zones(),
            "start_time": d_filter.get_start_time(),
            "end_time": d_filter.get_end_time()
        }

    def get_availability_stats(self, conn, d_filter, aggregation_level, group_by, aggregation_function):
        if group_by == 'modality':
            return self.get_availability_stats_per_modality(conn, d_filter, aggregation_level, aggregation_function)
        elif group_by == 'operator':
            return self.get_availability_stats_per_operator(conn, d_filter, aggregation_level, aggregation_function)

    def get_availability_stats_per_modality(self, conn, d_filter, aggregation_level, aggregation_function):
        agg_level = self.converted_aggregation_level(aggregation_level)

        query = """
            SELECT
                modality,
                time_bucket_gapfill(%(agg_level)s, time) AS bucket,
                {}(number_of_vehicles_parked) as amount
            FROM stats_number_of_vehicles_parked
            WHERE
                time >= (%(start_time)s) 
                AND time <= (%(end_time)s)
                AND zone_id IN (%(zone_ids)s)
            GROUP BY bucket, modality
            ORDER BY bucket ASC, modality
        """
        sql_query = query.format(aggregation_function)

        params = self.get_params(d_filter, aggregation_level)

        cur = conn.cursor()
        cur.execute(sql_query, params)
        rows = cur.fetchall()
        conn.commit()

        result = {}
        result["aggregation_level"] = agg_level
        result["values"] = self.populate_values(rows)
        return result

    def get_availability_stats_per_operator(self, conn, d_filter, aggregation_level, aggregation_function):
        agg_level = self.converted_aggregation_level(aggregation_level)

        query = """
            SELECT
                system_id,
                modality,
                time_bucket(%(agg_level)s, time) AS bucket,
                {}(number_of_vehicles_parked) as amount
            FROM stats_number_of_vehicles_parked
            WHERE
                time >= (%(start_time)s) 
                AND time <= (%(end_time)s)
                AND zone_id IN %(zone_ids)s
            GROUP BY bucket, system_id, modality
            ORDER BY bucket ASC, system_id
        """
        sql_query = query.format(aggregation_function)

        params = self.get_params(d_filter, agg_level)

        cur = conn.cursor()
        cur.execute(sql_query, params)
        rows = cur.fetchall()
        conn.commit()

        result = {}
        result["aggregation_level"] = agg_level
        result["values"] = self.populate_values(rows)
        return result

    # Function that groups the values array per datetime
    def populate_values(self, rows):
        # Create result variable
        result = []
        # Keep track of the last time you processed
        lastProcessedTime = None
        # Loop all rows and convert it into the preferred format
        time_values = {}
        for x in rows:
            name  = x[0] # i.e. modality name or operator name
            time  = x[2]
            count = x[3]
            # If we have this time in our result already: add key to object
            if lastProcessedTime == time:
                if name in time_values:
                    time_values[name] += count
                else:
                    time_values[name] = count
            # If this is a new time:
            else:
                # Push previous time object to result variable
                if lastProcessedTime:
                    result.append(time_values)
                # Start collecting data for this time
                time_values = {
                    'time': time.replace(tzinfo=None),
                    name: count
                }
                lastProcessedTime = time

        return result
