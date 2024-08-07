import datetime

class StatsOverTime():

    def query_stats(self, conn, filter):
        cur = conn.cursor()
        start_time = datetime.datetime.strptime(filter.get_start_time(), '%Y-%m-%dT%H:%M:%SZ')
        start_time = start_time.replace(hour=3, minute=0)
        end_time = datetime.datetime.strptime(filter.get_end_time(), '%Y-%m-%dT%H:%M:%SZ')
        end_time = end_time.replace(hour=3, minute = 0)
        date_generated = [start_time + datetime.timedelta(days=x) for x in range(0, ((end_time - start_time).days) + 1)]

        results = {}
        results["timestamp"] = []
        results["stats"] = []
        for timestamp in date_generated:
            results["timestamp"].append(timestamp)
            results["stats"].append(self.number_of_bicycles(timestamp, filter, cur))
        return results

    def number_of_bicycles(self, timestamp, d_filter, cur):
        stmt = """
        WITH temp_a (filter_area) AS
            (
            SELECT st_union(area) 
	            FROM zones WHERE zone_id IN %s
        )

        SELECT 
            CASE 
                WHEN datef < '2 DAYS' THEN 0
                WHEN datef >= '2 DAYS' and datef < '4 DAYS' THEN 1
                WHEN datef >= '4 DAYS' and datef < '7 DAYS' THEN 2
                WHEN datef >= '7 DAYS' and datef < '14 DAYS' THEN 3
                ELSE 4
            END as bucket,
            SUM(sum_bikes) as number_of_park_events
        FROM
        (SELECT date_trunc('day', %s - start_time) as datef, 
            count(1) as sum_bikes
        FROM (
            SELECT * 
            FROM park_events, temp_a
            WHERE start_time < %s 
            AND (end_time > %s or end_time is null)
            AND (false = %s or ST_WITHIN(location, temp_a.filter_area))
                AND (false = %s or system_id IN %s)) AS q1
            GROUP BY datef) q1
        GROUP BY bucket
        ORDER BY bucket;
            """
        cur.execute(stmt, (d_filter.get_zones(), 
            timestamp, timestamp, timestamp, 
            d_filter.has_zone_filter(),
            d_filter.has_operator_filter(), d_filter.get_operators()))
        
        result = self.extract_stat(cur.fetchall())
        return result


    def extract_stat(self, records):
        result = [0] * 6
        for record in records:
            result[record[0]] = int(record[1])
        return result
