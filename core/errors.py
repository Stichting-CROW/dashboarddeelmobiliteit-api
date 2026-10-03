from fastapi import FastAPI, Request
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse
from starlette.exceptions import HTTPException as StarletteHTTPException


def register_exception_handlers(app: FastAPI) -> None:
    @app.exception_handler(StarletteHTTPException)
    async def http_exception_handler(request: Request, exc: StarletteHTTPException):
        content = {"message": exc.detail}
        if exc.status_code == 401:
            content = {"code": exc.status_code, "message": exc.detail}
        return JSONResponse(
            status_code=exc.status_code,
            content=content,
            headers=getattr(exc, "headers", None),
        )

    @app.exception_handler(RequestValidationError)
    async def validation_exception_handler(request: Request, exc: RequestValidationError):
        errors = []
        for error in exc.errors():
            location = ".".join(str(part) for part in error.get("loc", []))
            errors.append("{}: {}".format(location, error.get("msg")))
        return JSONResponse(status_code=400, content={"message": "; ".join(errors)})
