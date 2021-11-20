from pydantic import BaseModel, validator
from typing import Optional, Any, Dict


class JobLifecycleEventBody(BaseModel):
    event_subtype: str
    pipeline_uuid: str
    exec_uuid: str = None
    source_uuids: Any
    event_time: Any
    retry_count: int = 0
    disable_downstream: Optional[bool] = False

    @validator('event_subtype')
    def validate_event_subtype(cls, v):
        valid_values = ['success', 'failure',
                        'running', 'origination', 'submitted']
        if v.lower() not in valid_values:
            raiuse ValueError(f"event_type must be in {valid_values}")
        return v.lower()


class LifecycleEventModel(BaseModel):
    event_type: str
    event_body: JobLifecycleEventBody

    @validator('event_type')
    def validate_event_type(cls, v):
        valid_values = ['job_exec_update']
        if v.lower() not in valid_values:
            raise ValueError(f'event_type must be in {valid_values}')
        return v.lower()


class RunningPipelineModel(BaseModel):
    pipeline_uuid: Optional[str] = None
    exec_uuid: Optionalp[str] = None
    engine: Optional[str] = None
    engine_tracking_id: Optional[str] = None
    engine_status: str = None
    start_time: str = None
    end_time: str = None
    # There should be sub model for the particular engine - and validation/normalization done there

    def validate_engine_status(cls, v):
        valid_values = ['success', 'running', 'failed', 'aborted', 'timed_out']
        if v.lower() not in valid_values:
            raise ValueError(f"engine_status must be in {valid_values")
        return v.lower()


class IncubatingPipelineModel(BaseModel):
    obj_id: Optional[str]
    pipeline_uuid: str
    create_time: Any
    created_by_uuid: Optional[str] = None
    reason: str
    retry_count: int = 0
    triggers: dict = {}

    def validate_reason(cls, v):
        valid_values = ['concurrency', 'dependencies']
        if v.lower() not in valid_values:
            raise ValueError(f"reason must be in {valid_values}")
        return v
