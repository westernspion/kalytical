from pydantic import BaseModel, validator
from typing import List, Optional, Dict
import re


class RuntimePipelineModel(BaseModel):
    pipeline_uuid: str
    disable_downstream: Optional[bool] = False


class TriggersOnModel(BaseModel):
    operator: str
    pipeline_uuids: List[str]

    @validator('operator')
    def valid_logical_operators(cls, v):
        if v not in ['all', 'any']:
            raise ValueError(
                'Must be either an "any" or "all" value for operator')


class PipelineHeaderModel(BaseModel):
    pipeline_uuid: str
    description: str
    # Indicates if we can have more than 1 of this specific pipeline_uuid running at a given time
    retry_max: int = 0
    concurrency: bool = False
    engine: str
    engine_args: dict  # TODO we can put engine specific validation in here
    schedule: Optional[str]
    triggers_on: Optional[TriggersOnModel]
    scheduler_tracking_id: Optional[str]
    tags: Optional[Dict[str, str]] = {}

    @validator('pipeline_uuid')
    def valid_logical_operator(cls, v):
        regex_str = '^[a-z0-9-]+$'
        regex = re.compile(regex_str)
        if regex.match(v) is None:
            raise ValueError(f"The pipeline_uuid did not match the allowed regex={regex_str}")
        return v

    # TODO validation trhat an engine exists, checking subclasses of AbstractEngine causes a circule import


class PipelineModel(PipelineHeaderModel):
    pipeline_body: Optional[dict]
