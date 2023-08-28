from pydantic import BaseModel

class ExportRequestParameters(BaseModel):
    start_time: str
    end_time: str
    
    filter_on_zones: bool
    zones: list[int]

    filter_on_operator: bool
    operators: list[str]

class ExportRequest(BaseModel):
    email: str
    query_parameters: ExportRequestParameters