from psycopg2 import sql

class RentalStats():
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

    def get_rental_stats(self, conn, d_filter, aggregation_level):
        agg_level = self.converted_aggregation_level(aggregation_level)

        query = """
            SELECT
                system_id,
                modality,
                time_bucket_gapfill(%(agg_level)s, time) AS bucket,
                COALESCE(SUM(number_of_trips_started), 0) as number_of_trips_started,
                COALESCE(SUM(number_of_trips_ended), 0) as number_of_trips_ended
            FROM stats_number_of_trips
            WHERE
                time >= (%(start_time)s) 
                AND time <= (%(end_time)s)
                AND zone_id IN %(zone_ids)s
            GROUP BY bucket, system_id, modality
            ORDER BY bucket ASC, system_id
        """

        params = self.get_params(d_filter, agg_level)

        cur = conn.cursor()
        cur.execute(query, params)
        rows = cur.fetchall()
        conn.commit()

        result = {}
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
        for row in rows:
            system_id  = row[0] # i.e. modality name or operator name
            modality = row[1]
            time  = row[2]
            rentals_started = row[3]
            rentals_ended = row[4]
            # If we have this time in our result already: add key to object
            if lastProcessedTime == time:
                if system_id in time_values:
                    time_values[system_id][modality] = {
                        'rentals_started': rentals_started,
                        'rentals_ended': rentals_ended
                    }
                else:
                    time_values[system_id] = {
                        modality: {
                            'rentals_started': rentals_started,
                            'rentals_ended': rentals_ended
                        }
                    }
            # If this is a new time:
            else:
                # Push previous time object to result variable
                if lastProcessedTime:
                    result.append(time_values)
                # Start collecting data for this time
                time_values = {
                    'time': time,
                    system_id: {
                        modality: {
                            'rentals_started': rentals_started,
                            'rentals_ended': rentals_ended
                        }
                    }
                }
                lastProcessedTime = time

        return result
