import datetime

# This class is a factory for generic filters.
class DataFilter():
    def __init__(self):
        self.zones = []
        self.timestamp = None
        self.start_time = None
        self.end_time = None

    def add_zones(self, args):
        if args.get("zone_ids"):
            self.zones = args.get("zone_ids").split(",")

    def get_zones(self):
        if len(self.zones) == 0:
            return (-1,)
        return tuple(self.zones)

    def has_zone_filter(self):
        return len(self.zones) > 0

    def add_timestamp(self, args):
        if args.get("timestamp"):
            self.timestamp = datetime.datetime.strptime(
                    args.get("timestamp"), "%Y-%m-%dT%H:%M:%SZ")

    def get_timestamp(self):
        return self.timestamp

    def has_timestamp(self):
        return self.timestamp

    def add_start_time(self, args):
        if args.get("start_time"):
            self.start_time = args.get("start_time")

    def get_start_time(self):
        return self.start_time

    def add_end_time(self, args):
        if args.get("end_time"):
            self.end_time = args.get("end_time")

    def get_end_time(self):
        return self.end_time

    @staticmethod
    def build(args):
        filter = DataFilter()
        filter.add_zones(args)
        filter.add_timestamp(args)
        filter.add_start_time(args)
        filter.add_end_time(args)

        return filter

