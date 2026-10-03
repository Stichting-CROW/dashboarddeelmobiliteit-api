from fastapi import APIRouter, HTTPException, Request

import audit_log
import export_raw_data.create_export_task
import stats_active_users
from core.filter_params import DataFilterDep
from core.security import AclUser, DbConn, not_authorized
from redis_helper import redis_helper

router = APIRouter()


@router.get("/raw_data")
def get_raw_data(request: Request, conn: DbConn, acl: AclUser, d_filter: DataFilterDep):
    if not d_filter.get_start_time():
        raise HTTPException(status_code=400, detail="No start_time specified")
    if not d_filter.get_end_time():
        raise HTTPException(status_code=400, detail="No end_time specified")

    d_filter.add_filters_based_on_acl(acl)

    authorized_acl, error = acl.is_authorized(d_filter)
    if not authorized_acl:
        return not_authorized(error)

    authorized = acl.is_authorized_for_raw_data()
    if not authorized:
        return not_authorized("This user is not admin and doesn't have raw data rights.")

    raw_api_call = request.url.path
    if request.url.query:
        raw_api_call += "?" + request.url.query
    audit_log.log_request(conn, acl.username, raw_api_call, d_filter)
    with redis_helper.get_resource() as r:
        result = export_raw_data.create_export_task.schedule_export(r, d_filter, acl.username)
        return result


@router.get("/menu/acl")
def show_human_readable_permission(conn: DbConn, acl: AclUser):
    cur2 = conn.cursor()
    result = acl.human_readable_serialize(cur2)
    stats_active_users.register_active_user(conn, result)
    return result
