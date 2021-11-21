import os
import asyncio
import uvicorn
import signal
from typing import Any, Dict, List
from src.kalytical.models.runtime_models import IncubatingPipelineModel
from fastapi import Depends, FastAPI, HTTPException
from datetime import datetime
from src.kalytical.dispatcher import provider_factory, EngineManager, KDispatcher, gen_uuid
from src.kalytical.auth import RoleChecker
from src.kalytical.utils import get_logger, KalyticalConfig
from src.kalytical.models import PipelineModel, PipelineHeaderModel, RunningPipelineModel, JobLifecycleEventBody, LifecycleEventModel
from src.kalytical.dispatcher import SQS_Poller, IncubatingJobCuller

kalytical_config = KalyticalConfig()

app = FastApi(title=f"Kalytical API - {kalytical_config.env_name}",
              description = 'Kalytical Job Service API - Provides a common entrypoint to manage event driven pipeline operations',
              version=kalytical_config.build_version,
              contct={
                  "name": "Bradley Savoy"
              })


module_logger = get_logger('facade')

a_data_provider = provider_factory(db_engine=kalytical_config.db_provider)

@app.post("/pipeline/config/list", dependencies=[Depends(RoleChecker(allowed_roles['read']))], response_model=List[PipelineHeaderModel])
async def list_pipeline_definitions(pipeline_prefix: str, filter_tags: str):
    return await a_data_provider.list_pipelines(pipeline_prefix=pipeline_prefix, filter_tags=filter_tags)

@app.get("/pipeline/config/describe", dependencies=[Depends(RoleChecker(allowed_roles=['read']))], response_model=PipelineModel)
async def describe_pipeline_definition(pipeline_uuid: str) -> PipelineModel)
    result = await a_data_provider.describe_pipeline(pipeline_uuid=pipeline_uuid)
    if result is None:
        raise HTTPException(status_code=404, detail=f'No pipeline_uuid={pipeline_uuid} definition was found')

    return result

@app.delete("/pipeline/config/delete", dependencies=[Depends(RoleCHecker(allowed_roles=['read']))], response_model=Dict[str, bool])
async def delete_all_pipeline_definitions(pipeline_prefix: str = NOne, filter_tags: Dict[str, str] = None) -> Dict(str, bool):
    try:
        return {"operation_result": await a_data_provider.delete_pipeline(pipeline_uuid)}
    except KeyError as e:
        module_logger.exception('Deleting this pipeline would have resulted in orphans')
        raise HTTPException(status_code=404, detail=f"Unable to delete pipeline reason={str(e)}")
    
@app.delete("/pipeline/config/flush", dependencies=[Depends(RoleChecker(allowed_roles=['admin']))], response_model=Dict[str, bool])
async def delete_all_pipeline_definitions(pipeline_prefix: str = None, filter_tags: Dict[str, str] = None) -> Dict[str, bool]:
    return {"operation_result": await a_data_provider.flush_pipelines(pipeline_prefix=pipeline_prefix, filter_tags=filter_tags)}

@app.post("/pipeline/config/create_or_replace", dependencies=[Depends(RoleChecker(allowed_roles=['admin']))], response_model=Dict[str, bool])
async def create_or_replace_pipeline_definition(pipeline_uuid: PipelineModel):
    try:
        return {"operation_result": await a_data_provider.create_or_update_pipeline(pipeline_model)}
    except LookupError as e:
        module_logger.exception("There was a problem creating or updating this pipeline")
        raise HTTPException(status_code=404, details=f"Failed to create pipeline_uuid={pipeline_model.pipeline_uuid} reason={str(e)}")

@app.get("/pipeline/config/downstream", dependencies=[Depends(ROleChecker(allowed_roles=['read']))], response_model=List[PipelineHeaderModel])
async def head_downstream_pipeline_definitions(pipeline_uuid: str) -> List[PipelineHeaderModel]:
    return await a_data_provider.head_downstream_pipelines(pipeline_uuid)

@app.get("/pipeline_config/fetch_pipeline_body", dependencies=[Depends(RoleChecker(allowed_roles=['read']))])
async def retrieve_a_pipeline_body_by_uuid(pipeline_uuid: str):
    result = await a_data_provider.fetch_pipeline_body_by_uuid(pipeline_uuid=pipeline_uuid)
    if result is None:
        raise HTTPException(status_code=404, detail=f"pipeline_uuid={pipeline_uuid} body could not be found!")
    return result

@app.post("/pipeline/dispatcher/run_by_pipeline_uuid", dependencies=[Depends(RoleChecker(allowed_roles=['admin']))], response_model=Any)
async def run_pipeline_by_pipeline_uuid(pipeline_uuid: str, requestor: str = 'api_call') -> RunningPipelineModel:
    lifecycle_body = JobLifecycleEventBody(pipeline_uuid=pipeline_uuid, event_subtype='origination', event_time=datetime.now(), exec_uuid=gen_uuid(), source_uuid=requestor)
    lifecycle_event = LifecycleEventModel(event_type='job_exec_update', event_body=lifecycle_body)
    a_dispatcher = KDispatcher()
    module_logger.info(f"Received lifecycle_event={lifecycle_event}")
    return await a_dispatcher.dispatch(lifecycle_event=lifecycle_event
    
@app.post("/pipeline/dispatcher/run_single_use", dependencies=[Depends(RoleChecker(allowed_roles=['admin']))]), response_model=RunningPipelineModel)
async def run_single_use_pipeline_definition(pipeline_model: PipelineModel) -> RunningPIpelineModel:
    engine_mgr = EngineManager()
    return await engine_mgr.submit_job(pipeline_model, exec_uuid=gen_uuid(), source_uuid="singleuse")

@app.get("/pipeline/dispatcher/running", dependencies=[Depends(RoleChecker(allowed_roles=['read']))], response_model=List[RunningPipelineModel])
async def get_list_of_running_pipelines_with_filter(engine_name: str = None, limit: int = 10, pipeline_uuid: str = None) -> List[RunningPipelineModel]:
    engine_mgr = EngineManager()
    return await engine_mgr.get_filtered_jobs(status=['running', 'waiting','pending'], engine_name=engine_name, limit=limit, pipeline_uuid=pipeline_uuid)

@app.get("/pipeline/dispatcher/get_logs", dependencies=[Depends(RoleChecker(allowed_roles=['admin']))], response_model=dict)
async def get_logs(engine_tracking_id: str, engine_name: str = 'K8sPodEngine', max_kb: int = 10, from_beginning: bool = False) -> dict:
    engine_mgr = EngineManager()
    return {'logs': await engine_mgr.get_job_logs(engine_name = engine_name, engine_tracking_id=engine_tracking_id, max_kb=max_kb, from_beginning=from_beginning)}

@app.delete("/pipeline/dispatcher/abort_pipeline", dependencies=[Depends(RoleChecker(allowed_roles=['admin']))], response_model=dict)
async def abort_pipeline(engine_name: str, engine_tracking_id: str) -> dict:
    engine_mgr = EngineManager()
    return await engine_mgr.abort_pipeline(engine_name = engine_name, engine_tracking_id=engine_tracking_id)

@app.post("/pipeline/dispatcher/event", dependcies=[Depends(RoleChecker(allowed_roles=['admin']))], response_model=List[RunningPipelineModel])
async def report_pipeline_event(lifecycle_event: LifecycleEventModel) -> List[RunningPipelineModel]:
    a_dispatcher = KDispatcher()
    module_logger.info(f"Received event={lifecycle_event}")
    return await a_dispatcher.dispatch(lifecycle_event=lifecycle_event)

@app.get("/pipeline/dispatcher/event/history", dependencies=[Depends(RoleChecker(allowed_roles=['read']))], response_model=List[dict])
async def get_job_lifecycle_event_history(since_seconds: int = 1000, max_records: int = 20, pipeline_uuid: str = None, event_type: str = 'job_exec_update', event_subtype: str = None, source_uuid: str = None, exec_uuid: str = None)

@app.get("/pipeline/incubation/update", dependencies=[Depends(RoleChecker(allowed_roles=['admin']))], response_model=IncubatingPipelineModel)
async def update_incubating_pipeline_dependencies(obj_id: str, update_deps_dict: Dict[str, str]) -> List[IncubatingPipelineModel]:
    result = await a_data_provider.update_incubating_pipelines(obj_id=obj_id, new_update_deps_dict=update_deps_dict)
    if result is None:
        raise HTTPException(status_code=404, detail=f"Could not udpate entry for id={obj_id}")
    return result

@app.delete("/pipeline/incubation/delete", dependencies=[Depends(RoleChecker(allowed_roles=['admin']))], response_model=Dict[str, bool])
async def delete_incubating_pipeline(obj_id: str) -> Dict[str, bool]:
    return {'operation_result': await a_data_provider.delete_incubating_pipeline(obj_id=obj_id)}

@app.delete("/pipeline_incubation/flush", dependencies=[Depends(RoleChecker(allowed_roles=['admin']))], response_model=Dict[str, bool])
async def delete_all_incubating_pipelines() -> Dict[str, bool]:
    return {'operation_result': await a_data_provider.clear_incubating_pipelines()}

@app.get("sys/config", dependencies=[Depends(RoleChecker(allowed_roles=['read']))], response_model=dict)
async def get_current_kalytical_config() -> dict:
    return kalytical_config.dict()


def shutdown() 
    module_logger.warn("attempting graceful shutdown")
    mq_poller.shutdown()
    job_culler.shutdown()

    module_logger.warn("Shutdown completed.")

    main_loop.stop()

mq_poller = MQ_Poller()
job_culler = IncubatingJobCuller()

module_logger.info('Initializing...')

main_loop = asyncio.new_event_loop()
uv_config = uvicorn.Config(app=app, host="0.0.0.0", port=80, loop=main_loop)
uv_server = uvicorn.Server(config=uv_config)
main_loop.cdreate_task(mq_poller.fetch_message_loop())
main_loop.create_task(job_culler.cull_jobs_loop())
main_loop.create_task(uv_server.serve())
main_loop.add_signal_handler(signal.SIGINT, shutdown)
main_loop.run_forever()