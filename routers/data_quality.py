from datetime import date
from typing import Optional

from fastapi import APIRouter, HTTPException, Query, Response
from typing_extensions import Annotated

from core.security import AclUser, DbConn, not_authorized
from data_quality import feed_status, rejections
from data_quality.feed_status import FeedAvailability
from data_quality.rejections import ManualRejection, ManualRejectionInput

router = APIRouter(prefix="/data_quality")

ADMIN_ONLY_MESSAGE = "Only admins can change manual rejections."

StartDate = Annotated[date, Query(description="Start date (inclusive)")]
EndDate = Annotated[date, Query(description="End date (inclusive)")]
GmCode = Annotated[
    Optional[str],
    Query(description="Municipality GM code; omit for all of The Netherlands"),
]


def _is_admin(acl) -> bool:
    return acl.organisation_type == "ADMIN"


def _check_query(acl, start_date: date, end_date: date, gm_code: Optional[str]):
    """Returns an error response, or None when the query is allowed."""
    if end_date < start_date:
        raise HTTPException(status_code=400, detail="end_date must be on or after start_date")
    if gm_code and not _is_admin(acl):
        authorized, error = acl.check_municipality_code(gm_code)
        if not authorized:
            return not_authorized(error)
    return None


def _operator_restriction(acl) -> Optional[list[str]]:
    """Operator accounts only see their own operators; None means no restriction."""
    if not _is_admin(acl) and len(acl.operator_filters) > 0:
        return sorted(acl.operator_filters)
    return None


@router.get("/feed_status", response_model=list[FeedAvailability])
def get_feed_status(
    conn: DbConn, acl: AclUser, start_date: StartDate, end_date: EndDate, gm_code: GmCode = None
):
    error = _check_query(acl, start_date, end_date, gm_code)
    if error:
        return error
    result = feed_status.get_feed_availability(
        conn, start_date, end_date, gm_code or None, _operator_restriction(acl)
    )
    conn.commit()
    return result


@router.get("/rejections", response_model=list[ManualRejection])
def get_rejections(
    conn: DbConn, acl: AclUser, start_date: StartDate, end_date: EndDate, gm_code: GmCode = None
):
    error = _check_query(acl, start_date, end_date, gm_code)
    if error:
        return error
    # Operator accounts also see rejections for all operators.
    return rejections.list_rejections(
        conn, start_date, end_date, gm_code or None, _operator_restriction(acl)
    )


@router.post("/rejections", response_model=ManualRejection, status_code=201)
def create_rejection(conn: DbConn, acl: AclUser, rejection: ManualRejectionInput):
    if not _is_admin(acl):
        return not_authorized(ADMIN_ONLY_MESSAGE)
    return rejections.create_rejection(conn, rejection, acl.username)


@router.put("/rejections/{rejection_id}", response_model=ManualRejection)
def update_rejection(
    rejection_id: int, conn: DbConn, acl: AclUser, rejection: ManualRejectionInput
):
    if not _is_admin(acl):
        return not_authorized(ADMIN_ONLY_MESSAGE)
    result = rejections.update_rejection(conn, rejection_id, rejection, acl.username)
    if result is None:
        raise HTTPException(status_code=404, detail="Rejection not found")
    return result


@router.delete("/rejections/{rejection_id}", status_code=204)
def delete_rejection(rejection_id: int, conn: DbConn, acl: AclUser):
    if not _is_admin(acl):
        return not_authorized(ADMIN_ONLY_MESSAGE)
    if not rejections.delete_rejection(conn, rejection_id, acl.username):
        raise HTTPException(status_code=404, detail="Rejection not found")
    return Response(status_code=204)
