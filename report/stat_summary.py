

class StatSummaryArea():
    def __init__(self, area, system_id):
        self.area = area
        self.system_id = system_id
        self.stats = {}

    def add_stat(self, stat_name, value):
        self.stats[stat_name] = value

    def get_stat(self, stat_name):
        if stat_name in self.stats:
            return self.stats[stat_name]
        return 0.0

class PreStatSummaryArea():
    def __init__(self, area, system_id):
        self.area = area
        self.system_id = system_id
        self.avg = {}
        self.count = {}
        self.sum = {}

    def add_stat(self, stat_name, avg_v, count_v, sum_v):
        self.avg[stat_name] = float(avg_v)
        self.count[stat_name] = float(count_v)
        self.sum[stat_name] = float(sum_v)

    def get_count(self, stat_name):
        if stat_name in self.count:
            return self.count[stat_name]
        return 0.0

    def get_avg(self, stat_name):
        if stat_name in self.avg:
            return self.avg[stat_name]
        return 0.0

    def get_sum(self, stat_name):
        if stat_name in self.sum:
            return self.sum[stat_name]
        return 0.0