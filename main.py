from fastapi import FastAPI

from core.database import lifespan
from core.errors import register_exception_handlers
from routers import account, park_events, public, rentals, reports, stats, trips, zones

app = FastAPI(
    title="Dashboard Deelmobiliteit API",
    lifespan=lifespan,
)
register_exception_handlers(app)

app.include_router(trips.router)
app.include_router(rentals.router)
app.include_router(zones.router)
app.include_router(park_events.router)
app.include_router(public.router)
app.include_router(stats.router)
app.include_router(reports.router)
app.include_router(account.router)
