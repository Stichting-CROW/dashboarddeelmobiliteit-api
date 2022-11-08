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
            "zone_ids": d_filter.get_zones()
        }

    def get_availability_stats(self, conn, d_filter, aggregation_level, group_by):
        if group_by == 'modality':
            return self.get_availability_stats_per_modality(conn, d_filter, aggregation_level)
        elif group_by == 'operator':
            return self.get_availability_stats_per_operator(conn, d_filter, aggregation_level)

    def get_availability_stats_per_modality(self, conn, d_filter, aggregation_level):
        agg_level = self.converted_aggregation_level(aggregation_level)

        query = """
            SELECT
                modality,
                time_bucket(%(agg_level)s, time) AS bucket,
                MAX(number_of_vehicles_parked) as amount
            FROM stats_number_of_vehicles_parked
            WHERE
                time > NOW() - INTERVAL '2 days'
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
        result["values"] = self.populate_values(rows)
        return result

    def get_availability_stats_per_operator(self, conn, d_filter, aggregation_level):
        agg_level = self.converted_aggregation_level(aggregation_level)

        query = """
            SELECT
                system_id,
                time_bucket(%(agg_level)s, time) AS bucket,
                MAX(number_of_vehicles_parked) as amount
            FROM stats_number_of_vehicles_parked
            WHERE
                time > NOW() - INTERVAL '2 days'
                AND zone_id IN %(zone_ids)s
            GROUP BY bucket, system_id
            ORDER BY bucket ASC, system_id
        """

        params = self.get_params(d_filter, agg_level)

        cur = conn.cursor()
        cur.execute(query, params)
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
            time  = x[1]
            count = x[2]
            # If we have this time in our result already: add key to object
            if lastProcessedTime == time:
                time_values[name] = count;
            # If this is a new time:
            else:
                # Push previous time object to result variable
                if lastProcessedTime:
                    result.append(time_values)
                # Start collecting data for this time
                time_values = {
                    'time': time,
                    name: count
                }
                lastProcessedTime = time

        return result;
