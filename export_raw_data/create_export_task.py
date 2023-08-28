import redis
import data_filter
import export_raw_data.export_request as export_request

def schedule_export(r: redis.Redis, d_filter: data_filter.DataFilter, email: str):
    exportRequestParameters = export_request.ExportRequestParameters(
        start_time = d_filter.start_time,
        end_time = d_filter.end_time,
        filter_on_zones = d_filter.has_zone_filter(),
        zones = d_filter.get_zones(),
        filter_on_operator = d_filter.has_operator_filter(),
        operators = d_filter.get_operators()
    )
    exportRequest = export_request.ExportRequest(
        email = email,
        query_parameters = exportRequestParameters
    )
    key = "export_raw_data_tasks"
    r.rpush(key, exportRequest.json())
    number_of_requests_in_queue = r.llen(key)
    return {
        "number_of_requests_in_queue": number_of_requests_in_queue,
        "email": email
    }
