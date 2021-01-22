import xlsxwriter
import psycopg2
import os
import datetime
import stat_summary
import report_stat_collector
import generate_stat_xlsx

conn_str = "dbname=deelfietsdashboard"
if "dev" in os.environ:
    conn_str = "dbname=deelfietsdashboard3"

if "ip" in os.environ:
    conn_str += " host={} ".format(os.environ['ip'])
if "password" in os.environ:
    conn_str += " user=deelfietsdashboard password={}".format(os.environ['password'])

conn = psycopg2.connect(conn_str)
gm_code = "GM0599"

rep = report_stat_collector.ReportStatCollector("GM0758", "2020-12-01", "2020-12-29")
report = rep.generate_report(conn)
print(report)

xlsx_generator = generate_stat_xlsx.XlsxGenerator()
print(report.get_active_operators())
for operator in report.get_active_operators():
    print(operator)
    xlsx_generator.add_sheet(operator, report)
resp = xlsx_generator.generate()


f = open('test.xlsx', 'wb')
f.write(resp)
f.close()