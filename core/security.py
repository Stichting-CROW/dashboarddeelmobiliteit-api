import jwt
from fastapi import Depends, HTTPException, Request
from fastapi.responses import JSONResponse
from typing_extensions import Annotated
from typing import Any

import access_control
from core.database import get_conn, get_timescaledb_conn

UNAUTHORIZED_MESSAGE = "You are not authorized (no token or invalid token is present)."


def get_username_from_request(request: Request):
    consumer_username = request.headers.get("X-Consumer-Username")
    authorization = request.headers.get("Authorization")
    if authorization:
        parts = authorization.split(" ")
        if len(parts) != 2:
            return None
        # Verification is performed by kong (reverse proxy),
        # therefore the token is not verified a second time so that the secret is only stored there.
        try:
            result = jwt.decode(parts[1], options={"verify_signature": False})
        except jwt.InvalidTokenError:
            return None
        return result.get("email")
    if consumer_username and consumer_username != "anonymous":
        return consumer_username
    return None


def get_acl_user(request: Request, conn: Any = Depends(get_conn)) -> access_control.ACL:
    username = get_username_from_request(request)
    if not username:
        raise HTTPException(status_code=401, detail=UNAUTHORIZED_MESSAGE)

    acl = access_control.AccessControl().retrieve_acl_user(username, conn)
    if not acl:
        raise HTTPException(status_code=401, detail=UNAUTHORIZED_MESSAGE)
    return acl


def not_authorized(error_msg):
    return JSONResponse(status_code=403, content={"error": error_msg})


DbConn = Annotated[Any, Depends(get_conn)]
TimeScaleConn = Annotated[Any, Depends(get_timescaledb_conn)]
AclUser = Annotated[access_control.ACL, Depends(get_acl_user)]
