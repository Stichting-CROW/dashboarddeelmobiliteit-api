import datetime
import json

# This class is a factory for generic filters.
class DataFilter():
    def __init__(self):
        self.zones = []
        self.operators = []
        self.timestamp = None
        self.start_time = None
        self.end_time = None
        self.gm_code = None
        self.form_factors = []

    def add_zones(self, args):
        if args.get("zone_ids"):
            self.zones = args.get("zone_ids").split(",")

    def add_zone(self, zone_id):
        self.zones.append(zone_id)

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

    def add_gmcode(self, args):
        if args.get("gm_code"):
            self.gm_code = args.get("gm_code")

    def get_gmcode(self):
        return self.gm_code
        
    def has_gmcode(self):
        return self.gm_code != None

    def add_operators(self, args):
        if args.get("operators"):
            self.operators = args.get("operators").split(",")

    def add_operator(self, operator_id):
        self.operators.append(operator_id)

    def has_operator_filter(self):
        return len(self.operators) > 0

    def get_operators(self):
        if len(self.operators) == 0:
            return ('undefined',)
        return tuple(self.operators)

    def add_form_factor(self, args):
        if args.get("form_factors"):
            self.form_factors = args.get("form_factors").split(",")

    def has_form_factor_filter(self):
        return len(self.form_factors) > 0

    def get_form_factors(self):
        if len(self.form_factors) == 0:
            return ('undefined',)
        return tuple(self.form_factors)

    def include_unknown_form_factors(self):
        return "unknown" in self.form_factors

    def add_filters_based_on_acl(self, acl):
        if acl.has_operator_filter():
            if len(acl.operator_filters) == 0:
                return "No operator_filters specified for this account while filtering on operator_filters is enabled."
            for operator_id in acl.operator_filters:
                self.add_operator(operator_id)
        if acl.has_municipality_filter():
            if len(acl.zone_filters) == 0:
                return "No municipalities specified on ACL for account while filtering on municipality is enabled."
            for zone_id in acl.zone_filters:
                self.add_zone(zone_id)

    def to_json(self):
        return json.dumps(self, default=lambda o: o.__dict__, 
            sort_keys=True, indent=4)

    @staticmethod
    def build(args):
        filter = DataFilter()
        filter.add_zones(args)
        filter.add_timestamp(args)
        filter.add_start_time(args)
        filter.add_end_time(args)
        filter.add_gmcode(args)
        filter.add_operators(args)
        filter.add_form_factor(args)

        return filter

