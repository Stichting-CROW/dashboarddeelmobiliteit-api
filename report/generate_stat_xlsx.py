import xlsxwriter
import psycopg2
import os
import datetime
from report import stat_summary, report_stat_collector
from io import BytesIO

class XlsxGenerator():
    def __init__(self):
        self.sio = BytesIO()
        self.workbook = xlsxwriter.Workbook(self.sio, {'in_memory': True}) 

    def generate(self):
        self.workbook.close()
        self.sio.seek(0)
        resp = self.sio.getvalue()	
        self.sio.close()	
        return resp

    def add_sheet(self, operator, report):
        sheet = XlsxSheet(self.workbook, operator)
        sheet.create(report)

class XlsxSheet():
    def __init__(self, workbook, operator):
        self.workbook = workbook
        self.operator = operator
        self.operator_name = operator
        if operator == None:
            self.operator_name = "Algemeen"
            self.operator = ""
        self.worksheet = workbook.add_worksheet(self.operator_name)
        # Add a bold format to use to highlight cells.
        self.bold = self.workbook.add_format({'bold': True})
        self.date_items = [
            {"letter": "a", "description": "Het aantal deelfietsen dat te huur wordt aangeboden in periode x en  in gebied y; Deze indicator wordt in twee stappen berekend. Eerst wordt elke nacht in periode x om 3:00 uur vastgesteld hoeveel deelfietsen in gebied y worden aangeboden. Vervolgens wordt het gemiddelde genomen van alle nachten in periode x.", "short_description": "aantal beschikbare voertuigen"},
            {"letter": "b", "description": "Aantal verhuringen per dag in periode x en in gebied y = som van de verhuringen in periode x  en in gebied y gedeeld door het aantal dagen in periode x.", "short_description": "verhuringen per dag"},
            {"letter": "c", "description": "Aantal verhuringen per fiets per dag in periode x  in gebied y = Indicator b gedeeld door indicator a.", "short_description": "aantal verhuringen per voertuig per dag."},
            {"letter": "d", "description": "Aantal deelfietsen dat langer 1 dag te huur staat in periode x en in gebied y = De som van het aantal te-huur-events die gestart zijn in periode x en in gebied y die een minimale duur hebben van 1 dag. Let op: deze indicator is pas stabiel als deze minimaal 1 dag na het verstrijken van periode x opgevraagd wordt.", "short_description": "aantal > 24 uur te huur"},
            {"letter": "e", "description": "Percentage van de  deelfietsen dat langer 1 dag te huur staat in periode x en in gebied y. Deze indicator wordt in twee stappen berekend. Eerst wordt elke nacht in periode x om 3:00 uur bepaald welk deel van de deelfietsen langer dan 1 dag te huur staat. Vervolgens wordt het gemiddelde genomen voor alle nachten in periode x.", "short_description": "% > 24 uur te huur"},
            {"letter": "f", "description": "Aantal deelfietsen dat langer 4 dagen te huur staat; Zie d.", "short_description": "aantal > 4 dagen te huur"},
            {"letter": "g", "description": "Percentage van de  deelfietsen dat langer 4 dagen te huur staat; Zie e", "short_description": "% > 4 dagen te huur"},
            {"letter": "h", "description": "Aantal deelfietsen dat langer 7 dagen te huur staat; Zie d.", "short_description": "aantal > 7 dagen te huur"},
            {"letter": "i", "description": "Percentage van de  deelfietsen dat langer 7 dagen te huur staat; Zie e.  ", "short_description": "% > 7 dagen te huur"},
            {"letter": "j", "description": "Gemiddelde verhuurduur voor voertuigen (eindigend) in zone.", "short_description": "Gemiddelde verhuurduur in minuten"}
        ]
        round_format = self.workbook.add_format({'num_format': '0'})
        one_decimal_format = self.workbook.add_format({'num_format': '0.0'})
        percentage_format = self.workbook.add_format({'num_format': '0%'})
        self.xlsx_formats = {
            "a": round_format,
            "b": round_format,
            "c": one_decimal_format,
            "d": round_format,
            "e": percentage_format,
            "f": round_format,
            "g": percentage_format,
            "h": round_format,
            "i": percentage_format,
            "j": one_decimal_format
        }
        self.cur_row = 0

    def create(self, report):
        # Widen the first column to make the text clearer.
        self.worksheet.set_column('A:A', 50)

        # Write some simple text.
        self.worksheet.write('A1', 'Deelfietsreportage periode ' + report.get_start_date() + ' - ' +  report.get_end_date(), self.bold)
        self.worksheet.write('B1', self.operator_name)
        self.cur_row += 1
        self.write_municipality(report)
        self.write_neighborhoods(report)
        self.write_legend()
       

    def write_municipality(self, report):
        # Text with formatting.
        self.worksheet.write('A3', 'Gemeente', self.bold)
        self.cur_row = 3
        cell_format = self.workbook.add_format()
        cell_format.set_rotation(30)
        for index, item in enumerate(self.date_items):
            self.worksheet.write(2, index + 1, item["letter"] + " " + item["short_description"], cell_format) 

        zones = report.get_zones()
        stats = report.get_result_status()
        self.worksheet.write('A4', zones[0][0])
        self.write_stat_row(stats[zones[0][1] + ":" + self.operator])

    def write_neighborhoods(self, report):
        self.worksheet.write('A6', 'Wijken', self.bold)
        self.cur_row = 6
        zones = report.get_zones()
        stats = report.get_result_status()
        print(stats)
        for index, zone in enumerate(zones[1:]):
            self.worksheet.write(self.cur_row, 0, zone[0])
            key = zone[1] + ":" + self.operator
            if key not in stats:
                self.write_stat_row(None)
            else:
                self.write_stat_row(stats[key],)

    def write_legend(self):
        self.cur_row += 3
        self.worksheet.write(self.cur_row, 0, 'Legenda', self.bold)
        self.cur_row += 1
        for date_item in self.date_items:
            text = date_item["letter"] + ": " + date_item["description"]
            self.worksheet.write(self.cur_row, 0, text)
            self.cur_row += 1

    def write_stat_row(self, stats):
        if not stats:
            self.cur_row += 1
            return
        for index, letter in enumerate(["a", "b", "c", "d", "e", "f", "g", "h", "i", "j"], 1):
            self.worksheet.write(self.cur_row, index, stats.get_stat(letter), self.xlsx_formats[letter])
        self.cur_row += 1
