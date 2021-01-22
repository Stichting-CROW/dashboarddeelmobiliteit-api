import psycopg2
import os
import datetime
from report import report_stat_collector, generate_stat_xlsx

def generate_report(conn, d_filter):
    rep = report_stat_collector.ReportStatCollector(d_filter)
    report = rep.generate_report(conn)

    xlsx_generator = generate_stat_xlsx.XlsxGenerator()
    print(report.get_active_operators())
    for operator in report.get_active_operators():
        print(operator)
        xlsx_generator.add_sheet(operator, report)
    resp = xlsx_generator.generate()
    file_name = "deelfietsrapportage_" + d_filter.get_start_time() + "_" + d_filter.get_end_time() + "_" + d_filter.get_gmcode()

    return resp, file_name

