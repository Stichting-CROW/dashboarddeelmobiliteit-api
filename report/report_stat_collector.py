import xlsxwriter
import psycopg2
import os
import datetime
from report import stat_summary


class ReportStatCollector():
    def __init__(self, d_filter):
        self.d_filter = d_filter
        self.number_of_days = None
        self.result = ReportStatResult(self.d_filter.get_start_date(), self.d_filter.get_end_date())

    def generate_report(self, conn):
        cur = conn.cursor()
        self.get_zones(cur)
        self.get_number_of_days_in_period(cur)
        stats = self.get_stats(cur)
        self.result.set_result_stats(stats)
        self.result.set_active_operators(self.get_active_operators(cur))
        return self.result

    def get_zones(self, cur):
        stmt = """SELECT name, stats_ref, zone_type 
            FROM zones 
            WHERE municipality = %s
            AND zone_type in ('municipality', 'residential_area') 
            ORDER BY zone_type, name;
        """
        cur.execute(stmt, (self.d_filter.get_gmcode(),))
        zones = cur.fetchall()
        self.result.set_zones(zones)

    def get_number_of_days_in_period(self, cur):
        stmt = """SELECT generate_series(
           (date %s)::timestamp,
           (date %s)::timestamp,
           interval '1 days'
        )::date"""
        cur.execute(stmt, (self.d_filter.get_start_time(), self.d_filter.get_end_time(),))
        self.number_of_days = len(cur.fetchall())

    def get_stats(self, cur):
        stmt = """SELECT zone_ref, stat_description, system_id, avg(value) as avg, count(value) as count, sum(value) as sum
            FROM stats_pre_process
            WHERE date
            IN (
                SELECT generate_series(
                    (date %s)::timestamp,
                    (date %s)::timestamp,
                    interval '1 days'
                )::date
            ) 
            AND zone_ref in (select stats_ref from zones where municipality = %s)
            AND (false = %s or system_id IN %s) 
            GROUP BY zone_ref, stat_description, system_id
            ORDER BY avg;
            """
        cur.execute(stmt, (self.d_filter.get_start_time(),  self.d_filter.get_end_time(), 
            self.d_filter.get_gmcode(), self.d_filter.has_operator_filter(), self.d_filter.get_operators().append("")))
        stats = cur.fetchall()
        pre_processed_stats = self.pre_process_stats(stats)
        return self.process_stats(pre_processed_stats)

    def pre_process_stats(self, stats):
        pre_result_stats = {}
        for stat in stats:
            zone_ref = stat[0]
            system_id = stat[2]
            key = zone_ref + ":"
            if system_id:
                key = zone_ref + ":" + system_id
            
            if key not in pre_result_stats:
                pre_result_stats[key] = stat_summary.PreStatSummaryArea(zone_ref, system_id)
            print(key + ' ' + stat[1] + ' ' + str(stat[3]) + ' ' + str(stat[4]) + ' ' + str(stat[5]))
            pre_result_stats[key].add_stat(stat[1], stat[3], stat[4], stat[5])
        return pre_result_stats

    def process_stats(self, pre_processed_stats):
        print("process_stats")
        result = {}
        for key, value in pre_processed_stats.items():
            newSummary = stat_summary.StatSummaryArea(value.area, value.system_id)
            newSummary.add_stat("a", value.get_sum("number_of_vehicles_available") / self.number_of_days)
            newSummary.add_stat("b", value.get_sum("number_of_trip_started") / self.number_of_days)
            if newSummary.get_stat("a") > 0.0:
                newSummary.add_stat("c", newSummary.get_stat("b") / newSummary.get_stat("a"))
            print(key)
            print(value.get_sum("number_of_vehicles_available_longer_then_24_hours"))
            newSummary.add_stat("d", value.get_sum("number_of_vehicles_available_longer_then_24_hours") / self.number_of_days)
            newSummary.add_stat("f", value.get_sum("number_of_vehicles_available_longer_then_4_days") / self.number_of_days)
            
            newSummary.add_stat("h", value.get_sum("number_of_vehicles_available_longer_then_7_days") / self.number_of_days)
            if value.get_sum("number_of_vehicles_available") > 0.0: 
                newSummary.add_stat("e", (value.get_sum("number_of_vehicles_available_longer_then_24_hours") / value.get_sum("number_of_vehicles_available")))
                newSummary.add_stat("g", (value.get_sum("number_of_vehicles_available_longer_then_4_days") / value.get_sum("number_of_vehicles_available")))
                newSummary.add_stat("i", (value.get_sum("number_of_vehicles_available_longer_then_7_days") / value.get_sum("number_of_vehicles_available")))
            if value.get_sum("number_of_trips_ended") > 0.0:
                newSummary.add_stat("j", (value.get_sum("sum_of_trip_duration") / value.get_sum("number_of_trips_ended")) / 60.0)
            result[key] = newSummary
        return result
            
    def get_active_operators(self, cur):
        stmt = """SELECT distinct(system_id)
        FROM stats_pre_process
        WHERE date
        IN (
            SELECT generate_series(
                (date %s)::timestamp,
                (date %s)::timestamp,
                interval '1 days'
            )::date
        ) 
        AND zone_ref in (select stats_ref from zones where municipality = %s)
        AND (false = %s or system_id IN %s) ;
        """
        cur.execute(stmt, (self.d_filter.get_start_date(), self.d_filter.get_end_date(), 
            self.d_filter.get_gmcode(), self.d_filter.has_operator_filter(), self.d_filter.get_operator_filter().append("")))
        operators = cur.fetchall()
        operators = [operator[0] for operator in operators]
        return operators

    def get_result(self):
        return self.result

class ReportStatResult():
    def __init__(self, start_date, end_date):
        self.zones = []
        self.active_operators = []
        self.result_stats = {}
        self.start_date = start_date
        self.end_date = end_date

    def set_zones(self, zones):
        self.zones = zones

    def set_result_stats(self, result_stats):
        self.result_stats = result_stats

    def set_active_operators(self, new_active_operators):
        self.active_operators = new_active_operators

    def get_active_operators(self):
        return self.active_operators

    def get_zones(self):
        return self.zones

    def get_start_date(self):
        return self.start_date

    def get_end_date(self):
        return self.end_date

    def get_result_status(self):
        return self.result_stats






