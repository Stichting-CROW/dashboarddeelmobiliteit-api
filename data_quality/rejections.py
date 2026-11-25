from datetime import date, datetime, timedelta
from typing import Literal, Optional, Union

from pydantic import BaseModel, field_validator, model_validator

ALL = "all"
TIMEZONE = "Europe/Amsterdam"

DataType = Literal["aanbod", "verhuringen", "servicegebieden"]
IssueOwner = Literal["operator", "dashboard_deelmobiliteit"]

# The database stores feed_data_type values; the API uses the Dutch names.
DB_DATA_TYPES = {
    "aanbod": "vehicles",
    "verhuringen": "trips",
    "servicegebieden": "service_areas",
}
API_DATA_TYPES = {db_value: api_value for api_value, db_value in DB_DATA_TYPES.items()}


class ManualRejectionInput(BaseModel):
    system_id: str
    start_time: datetime
    end_time: Optional[datetime] = None
    municipalities: Union[Literal["all"], list[str]]
    vehicle_types: Union[Literal["all"], list[str]]
    data_types: list[DataType]
    issue_owner: IssueOwner
    note: str = ""

    @field_validator("system_id")
    @classmethod
    def system_id_not_empty(cls, value: str) -> str:
        value = value.strip()
        if not value:
            raise ValueError("choose an operator or 'all'")
        return value

    @field_validator("municipalities", "vehicle_types")
    @classmethod
    def list_not_empty(cls, value):
        if value != ALL:
            value = list(dict.fromkeys(item.strip() for item in value if item.strip()))
            if not value:
                raise ValueError("choose at least one value or 'all'")
        return value

    @field_validator("data_types")
    @classmethod
    def data_types_not_empty(cls, value: list[str]) -> list[str]:
        value = list(dict.fromkeys(value))
        if not value:
            raise ValueError("choose at least one data type")
        return value

    @field_validator("start_time", "end_time")
    @classmethod
    def has_timezone(cls, value: Optional[datetime]) -> Optional[datetime]:
        if value is not None and value.tzinfo is None:
            raise ValueError("include a timezone, e.g. 2026-11-25T08:00:00Z")
        return value

    @model_validator(mode="after")
    def end_after_start(self):
        if self.end_time is not None and self.end_time <= self.start_time:
            raise ValueError("end_time must be after start_time")
        self.note = self.note.strip()
        return self


class ManualRejection(ManualRejectionInput):
    rejection_id: int
    created_by: str
    created_at: datetime
    updated_by: Optional[str] = None
    updated_at: Optional[datetime] = None


def _to_db_list(value):
    return None if value == ALL else value


def _to_db_params(rejection: ManualRejectionInput) -> dict:
    return {
        "system_id": None if rejection.system_id == ALL else rejection.system_id,
        "start_time": rejection.start_time,
        "end_time": rejection.end_time,
        "municipalities": _to_db_list(rejection.municipalities),
        "modalities": _to_db_list(rejection.vehicle_types),
        "data_types": [DB_DATA_TYPES[data_type] for data_type in rejection.data_types],
        "issue_owner": rejection.issue_owner,
        "note": rejection.note,
    }


SELECT_COLUMNS = """
    rejection_id, system_id, start_time, end_time, municipalities, modalities,
    data_types::text[], issue_owner, note, created_by, created_at, updated_by, updated_at
"""


def _from_row(row) -> ManualRejection:
    return ManualRejection(
        rejection_id=row[0],
        system_id=row[1] if row[1] is not None else ALL,
        start_time=row[2],
        end_time=row[3],
        municipalities=row[4] if row[4] is not None else ALL,
        vehicle_types=row[5] if row[5] is not None else ALL,
        data_types=[API_DATA_TYPES[data_type] for data_type in row[6]],
        issue_owner=row[7],
        note=row[8],
        created_by=row[9],
        created_at=row[10],
        updated_by=row[11],
        updated_at=row[12],
    )


def list_rejections(
    conn,
    start_date: date,
    end_date: date,
    gm_code: Optional[str],
    operators: Optional[list[str]],
) -> list[ManualRejection]:
    """
    Rejections that overlap [start_date, end_date] (both inclusive, Dutch local time).
    `operators` limits the result to rejections for those operators or for all
    operators; None means no limit.
    """
    stmt = f"""
        SELECT {SELECT_COLUMNS}
        FROM data_quality_rejection
        WHERE deleted_at IS NULL
        AND start_time < (%(end_date)s::timestamp AT TIME ZONE %(tz)s)
        AND (end_time IS NULL OR end_time > (%(start_date)s::timestamp AT TIME ZONE %(tz)s))
        AND (%(gm_code)s::text IS NULL OR municipalities IS NULL OR %(gm_code)s = ANY(municipalities))
        AND (%(operators)s::text[] IS NULL OR system_id IS NULL OR system_id = ANY(%(operators)s))
        ORDER BY start_time DESC, rejection_id DESC
    """
    cur = conn.cursor()
    cur.execute(stmt, {
        "start_date": start_date,
        "end_date": end_date + timedelta(days=1),
        "tz": TIMEZONE,
        "gm_code": gm_code,
        "operators": operators,
    })
    result = [_from_row(row) for row in cur.fetchall()]
    conn.commit()
    return result


def create_rejection(conn, rejection: ManualRejectionInput, username: str) -> ManualRejection:
    stmt = f"""
        INSERT INTO data_quality_rejection (
            system_id, start_time, end_time, municipalities, modalities,
            data_types, issue_owner, note, created_by
        ) VALUES (
            %(system_id)s, %(start_time)s, %(end_time)s, %(municipalities)s, %(modalities)s,
            %(data_types)s::feed_data_type[], %(issue_owner)s, %(note)s, %(username)s
        )
        RETURNING {SELECT_COLUMNS}
    """
    cur = conn.cursor()
    cur.execute(stmt, {**_to_db_params(rejection), "username": username})
    row = cur.fetchone()
    conn.commit()
    return _from_row(row)


def update_rejection(
    conn, rejection_id: int, rejection: ManualRejectionInput, username: str
) -> Optional[ManualRejection]:
    stmt = f"""
        UPDATE data_quality_rejection
        SET system_id = %(system_id)s,
            start_time = %(start_time)s,
            end_time = %(end_time)s,
            municipalities = %(municipalities)s,
            modalities = %(modalities)s,
            data_types = %(data_types)s::feed_data_type[],
            issue_owner = %(issue_owner)s,
            note = %(note)s,
            updated_by = %(username)s,
            updated_at = NOW()
        WHERE rejection_id = %(rejection_id)s AND deleted_at IS NULL
        RETURNING {SELECT_COLUMNS}
    """
    cur = conn.cursor()
    cur.execute(stmt, {
        **_to_db_params(rejection),
        "username": username,
        "rejection_id": rejection_id,
    })
    row = cur.fetchone()
    conn.commit()
    return _from_row(row) if row else None


def delete_rejection(conn, rejection_id: int, username: str) -> bool:
    """Soft delete, so it stays visible who removed the rejection."""
    stmt = """
        UPDATE data_quality_rejection
        SET deleted_by = %(username)s, deleted_at = NOW()
        WHERE rejection_id = %(rejection_id)s AND deleted_at IS NULL
    """
    cur = conn.cursor()
    cur.execute(stmt, {"username": username, "rejection_id": rejection_id})
    deleted = cur.rowcount > 0
    conn.commit()
    return deleted
