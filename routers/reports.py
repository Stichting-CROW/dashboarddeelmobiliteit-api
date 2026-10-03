import io

from fastapi import APIRouter, HTTPException
from fastapi.responses import StreamingResponse

import report.generate_xlsx
from core.filter_params import DataFilterDep
from core.security import AclUser, DbConn, not_authorized

router = APIRouter()

XLSX_MIMETYPE = "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"


@router.get("/stats/generate_report")
def get_report(conn: DbConn, acl: AclUser, d_filter: DataFilterDep):
    if not d_filter.has_gmcode():
        raise HTTPException(status_code=400, detail="No municipality specified")
    if not d_filter.get_start_time():
        raise HTTPException(status_code=400, detail="No start_time specified")
    if not d_filter.get_end_time():
        raise HTTPException(status_code=400, detail="No end_time specified")

    authorized, error = acl.check_municipality_code(d_filter.get_gmcode())
    if not authorized:
        return not_authorized(error)
    authorized, error = acl.check_operators(d_filter)
    if not authorized:
        return not_authorized(error)

    raw_data, file_name = report.generate_xlsx.generate_report(conn, d_filter)
    headers = {
        "Content-Disposition": 'attachment; filename="{}.xlsx"'.format(file_name),
        "Cache-Control": "no-cache",
    }
    return StreamingResponse(io.BytesIO(raw_data), media_type=XLSX_MIMETYPE, headers=headers)
